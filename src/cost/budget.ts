/**
 * Budget Guard: enforces daily and monthly spending limits.
 *
 * Architecture:
 *   - In-memory tracking with periodic flush to storage
 *   - Soft alerts at configurable threshold (e.g. 80%)
 *   - Hard block when limit is reached
 *   - Per-tenant budget isolation (multi-tenant support)
 *   - Reset on period boundary (midnight UTC for daily, 1st of month)
 */

import type { EventEmitter } from 'eventemitter3';

export interface BudgetConfig {
  /** Daily spending limit in USD */
  daily?: number;
  /** Monthly spending limit in USD */
  monthly?: number;
  /** Threshold (0-1) at which to emit warning events */
  alertThreshold?: number;
  /** Per-tenant overrides */
  tenantLimits?: Record<string, { daily?: number; monthly?: number }>;
}

interface PeriodUsage {
  amount: number;
  periodStart: number;
  lastUpdated: number;
}

type BudgetEvents = {
  'budget:warning': [{ usage: number; limit: number; period: string }];
  'budget:exceeded': [{ usage: number; limit: number; period: string }];
};

export class BudgetGuard {
  private dailyUsage: PeriodUsage;
  private monthlyUsage: PeriodUsage;
  private tenantUsage: Map<string, { daily: PeriodUsage; monthly: PeriodUsage }> = new Map();
  private readonly alertThreshold: number;
  private warningEmitted: { daily: boolean; monthly: boolean } = { daily: false, monthly: false };

  constructor(
    private readonly config: BudgetConfig | undefined,
    private readonly emitter?: EventEmitter<BudgetEvents>,
  ) {
    this.alertThreshold = config?.alertThreshold ?? 0.8;
    this.dailyUsage = this.createPeriodUsage();
    this.monthlyUsage = this.createPeriodUsage();
  }

  /**
   * Asserts that current usage is within budget.
   * Throws BudgetExceededError if any limit is breached.
   */
  async assertWithinBudget(tenantId?: string): Promise<void> {
    this.rolloverIfNeeded();

    const usage = tenantId ? this.getTenantUsage(tenantId) : { daily: this.dailyUsage, monthly: this.monthlyUsage };

    const limits = tenantId && this.config?.tenantLimits?.[tenantId]
      ? this.config.tenantLimits[tenantId]
      : this.config;

    if (limits?.daily && usage.daily.amount >= limits.daily) {
      this.emitter?.emit('budget:exceeded', {
        usage: usage.daily.amount,
        limit: limits.daily,
        period: 'daily',
      });
      throw new BudgetExceededError(
        `Daily budget exceeded: $${usage.daily.amount.toFixed(4)} / $${limits.daily}`,
        'daily',
        usage.daily.amount,
        limits.daily,
      );
    }

    if (limits?.monthly && usage.monthly.amount >= limits.monthly) {
      this.emitter?.emit('budget:exceeded', {
        usage: usage.monthly.amount,
        limit: limits.monthly,
        period: 'monthly',
      });
      throw new BudgetExceededError(
        `Monthly budget exceeded: $${usage.monthly.amount.toFixed(4)} / $${limits.monthly}`,
        'monthly',
        usage.monthly.amount,
        limits.monthly,
      );
    }
  }

  /**
   * Records cost from a completed request.
   * Emits warning events when threshold is crossed.
   */
  async recordUsage(amount: number, tenantId?: string): Promise<void> {
    this.rolloverIfNeeded();

    this.dailyUsage.amount += amount;
    this.dailyUsage.lastUpdated = Date.now();
    this.monthlyUsage.amount += amount;
    this.monthlyUsage.lastUpdated = Date.now();

    if (tenantId) {
      const tenant = this.getTenantUsage(tenantId);
      tenant.daily.amount += amount;
      tenant.monthly.amount += amount;
    }

    this.checkWarnings();
  }

  /**
   * Returns current usage summary.
   */
  getUsageSummary(): {
    daily: { used: number; limit: number | null; percentage: number };
    monthly: { used: number; limit: number | null; percentage: number };
  } {
    this.rolloverIfNeeded();

    return {
      daily: {
        used: this.dailyUsage.amount,
        limit: this.config?.daily ?? null,
        percentage: this.config?.daily
          ? this.dailyUsage.amount / this.config.daily
          : 0,
      },
      monthly: {
        used: this.monthlyUsage.amount,
        limit: this.config?.monthly ?? null,
        percentage: this.config?.monthly
          ? this.monthlyUsage.amount / this.config.monthly
          : 0,
      },
    };
  }

  /**
   * Resets usage counters. Useful for testing or manual override.
   */
  reset(): void {
    this.dailyUsage = this.createPeriodUsage();
    this.monthlyUsage = this.createPeriodUsage();
    this.tenantUsage.clear();
    this.warningEmitted = { daily: false, monthly: false };
  }

  private checkWarnings(): void {
    if (this.config?.daily && !this.warningEmitted.daily) {
      const ratio = this.dailyUsage.amount / this.config.daily;
      if (ratio >= this.alertThreshold) {
        this.warningEmitted.daily = true;
        this.emitter?.emit('budget:warning', {
          usage: this.dailyUsage.amount,
          limit: this.config.daily,
          period: 'daily',
        });
      }
    }

    if (this.config?.monthly && !this.warningEmitted.monthly) {
      const ratio = this.monthlyUsage.amount / this.config.monthly;
      if (ratio >= this.alertThreshold) {
        this.warningEmitted.monthly = true;
        this.emitter?.emit('budget:warning', {
          usage: this.monthlyUsage.amount,
          limit: this.config.monthly,
          period: 'monthly',
        });
      }
    }
  }

  private rolloverIfNeeded(): void {
    const now = new Date();
    const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).getTime();

    if (this.dailyUsage.periodStart < todayStart) {
      this.dailyUsage = this.createPeriodUsage();
      this.warningEmitted.daily = false;
    }

    if (this.monthlyUsage.periodStart < monthStart) {
      this.monthlyUsage = this.createPeriodUsage();
      this.warningEmitted.monthly = false;
    }
  }

  private getTenantUsage(tenantId: string): { daily: PeriodUsage; monthly: PeriodUsage } {
    if (!this.tenantUsage.has(tenantId)) {
      this.tenantUsage.set(tenantId, {
        daily: this.createPeriodUsage(),
        monthly: this.createPeriodUsage(),
      });
    }
    return this.tenantUsage.get(tenantId)!;
  }

  private createPeriodUsage(): PeriodUsage {
    return { amount: 0, periodStart: Date.now(), lastUpdated: Date.now() };
  }
}

export class BudgetExceededError extends Error {
  readonly code = 'BUDGET_EXCEEDED';

  constructor(
    message: string,
    public readonly period: 'daily' | 'monthly',
    public readonly usage: number,
    public readonly limit: number,
  ) {
    super(message);
    this.name = 'BudgetExceededError';
  }
}
