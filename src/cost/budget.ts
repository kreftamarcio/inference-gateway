/**
 * Budget guard: enforces daily and monthly spending limits.
 *
 * Design:
 *   - In-memory tracking, with UTC period boundaries
 *   - Soft alert at a configurable threshold, hard block at the limit
 *   - Per-tenant isolation, each tenant with its own periods and warning state
 *
 * Boundaries are UTC deliberately. Local-time boundaries make the window shift with
 * the host's offset, so the same code enforces a different window depending on where
 * it runs, and a deployment that moves regions silently changes billing periods.
 */

import type { EventEmitter } from 'eventemitter3';

export interface BudgetConfig {
  /** Daily spending limit in USD. */
  daily?: number;
  /** Monthly spending limit in USD. */
  monthly?: number;
  /** Fraction of the limit at which a warning is emitted. Defaults to 0.8. */
  alertThreshold?: number;
  /** Per-tenant overrides. A tenant without an entry inherits the global limits. */
  tenantLimits?: Record<string, { daily?: number; monthly?: number }>;
}

interface PeriodUsage {
  amount: number;
  /** Start of the current period, as a UTC boundary timestamp. */
  periodStart: number;
  lastUpdated: number;
}

/** Usage plus warning state for one scope: global, or a single tenant. */
interface Scope {
  daily: PeriodUsage;
  monthly: PeriodUsage;
  warned: { daily: boolean; monthly: boolean };
}

type BudgetEvents = {
  'budget:warning': [{ usage: number; limit: number; period: string; tenantId?: string }];
  'budget:exceeded': [{ usage: number; limit: number; period: string; tenantId?: string }];
};

export class BudgetGuard {
  private readonly global: Scope;
  private readonly tenants = new Map<string, Scope>();
  private readonly alertThreshold: number;

  constructor(
    private readonly config: BudgetConfig | undefined,
    private readonly emitter?: EventEmitter<BudgetEvents>,
  ) {
    const threshold = config?.alertThreshold ?? 0.8;

    if (threshold <= 0 || threshold > 1) {
      throw new RangeError(
        `alertThreshold must be within (0,1], received ${threshold}. A threshold above 1 ` +
          'would never fire, and one at or below 0 would fire on the first request.',
      );
    }

    this.alertThreshold = threshold;
    this.global = this.createScope();
  }

  /**
   * Throw if the relevant limits are already reached.
   *
   * Checked BEFORE the request, not after: checking afterwards means every run exceeds
   * its ceiling by one inference, and inference is the most expensive operation in the
   * system.
   */
  async assertWithinBudget(tenantId?: string): Promise<void> {
    const scope = this.scopeFor(tenantId);
    this.rollover(scope);

    // Limits and usage must come from the same scope. Comparing tenant usage against a
    // global limit, or the reverse, produces an enforcement decision about a number
    // that was never accumulated for that scope.
    const limits = this.limitsFor(tenantId);

    if (limits.daily !== undefined && scope.daily.amount >= limits.daily) {
      this.emit('budget:exceeded', scope.daily.amount, limits.daily, 'daily', tenantId);
      throw new BudgetExceededError(
        `Daily budget exceeded: $${scope.daily.amount.toFixed(4)} / $${limits.daily}` +
          (tenantId ? ` (tenant ${tenantId})` : ''),
        'daily',
        scope.daily.amount,
        limits.daily,
        tenantId,
      );
    }

    if (limits.monthly !== undefined && scope.monthly.amount >= limits.monthly) {
      this.emit('budget:exceeded', scope.monthly.amount, limits.monthly, 'monthly', tenantId);
      throw new BudgetExceededError(
        `Monthly budget exceeded: $${scope.monthly.amount.toFixed(4)} / $${limits.monthly}` +
          (tenantId ? ` (tenant ${tenantId})` : ''),
        'monthly',
        scope.monthly.amount,
        limits.monthly,
        tenantId,
      );
    }
  }

  /**
   * Record cost from a completed request.
   *
   * A tenant request updates both the tenant scope and the global scope. Without the
   * global update, an account-wide ceiling could be blown by the sum of tenants that
   * each stayed inside their own allowance.
   */
  async recordUsage(amount: number, tenantId?: string): Promise<void> {
    if (!Number.isFinite(amount) || amount < 0) {
      throw new RangeError(
        `Usage amount must be a non-negative finite number, received ${amount}. A negative ` +
          'amount would silently create budget headroom.',
      );
    }

    const now = Date.now();

    this.rollover(this.global);
    this.global.daily.amount += amount;
    this.global.monthly.amount += amount;
    this.global.daily.lastUpdated = now;
    this.global.monthly.lastUpdated = now;

    if (tenantId !== undefined) {
      const tenant = this.scopeFor(tenantId);
      this.rollover(tenant);
      tenant.daily.amount += amount;
      tenant.monthly.amount += amount;
      tenant.daily.lastUpdated = now;
      tenant.monthly.lastUpdated = now;

      // Tenant warnings before global, so a listener sees the more specific signal
      // first. Amounts are already recorded, so a listener reading getUsageSummary
      // during the event sees the value that triggered it.
      this.checkWarnings(tenant, this.limitsFor(tenantId), tenantId);
    }

    this.checkWarnings(this.global, this.limitsFor(undefined), undefined);
  }

  getUsageSummary(tenantId?: string): {
    daily: { used: number; limit: number | null; percentage: number; resetsAt: string };
    monthly: { used: number; limit: number | null; percentage: number; resetsAt: string };
  } {
    const scope = this.scopeFor(tenantId);
    this.rollover(scope);

    const limits = this.limitsFor(tenantId);

    return {
      daily: {
        used: scope.daily.amount,
        limit: limits.daily ?? null,
        percentage: limits.daily ? scope.daily.amount / limits.daily : 0,
        resetsAt: new Date(this.nextDailyBoundary()).toISOString(),
      },
      monthly: {
        used: scope.monthly.amount,
        limit: limits.monthly ?? null,
        percentage: limits.monthly ? scope.monthly.amount / limits.monthly : 0,
        resetsAt: new Date(this.nextMonthlyBoundary()).toISOString(),
      },
    };
  }

  /** Every tracked tenant, for a spend dashboard. */
  getTenantSummaries(): Array<{ tenantId: string; daily: number; monthly: number }> {
    return [...this.tenants.entries()].map(([tenantId, scope]) => {
      this.rollover(scope);
      return { tenantId, daily: scope.daily.amount, monthly: scope.monthly.amount };
    });
  }

  reset(tenantId?: string): void {
    if (tenantId !== undefined) {
      this.tenants.delete(tenantId);
      return;
    }

    this.tenants.clear();
    Object.assign(this.global, this.createScope());
  }

  /**
   * Resolve limits for a scope.
   *
   * A tenant override is merged over the global config field by field, so a tenant that
   * overrides only `daily` still inherits the global `monthly`. Replacing the whole
   * object would silently drop the monthly ceiling for that tenant.
   */
  private limitsFor(tenantId: string | undefined): { daily?: number; monthly?: number } {
    if (tenantId === undefined) {
      return {
        ...(this.config?.daily !== undefined ? { daily: this.config.daily } : {}),
        ...(this.config?.monthly !== undefined ? { monthly: this.config.monthly } : {}),
      };
    }

    const override = this.config?.tenantLimits?.[tenantId];

    const daily = override?.daily ?? this.config?.daily;
    const monthly = override?.monthly ?? this.config?.monthly;

    return {
      ...(daily !== undefined ? { daily } : {}),
      ...(monthly !== undefined ? { monthly } : {}),
    };
  }

  private scopeFor(tenantId: string | undefined): Scope {
    if (tenantId === undefined) return this.global;

    let scope = this.tenants.get(tenantId);
    if (!scope) {
      scope = this.createScope();
      this.tenants.set(tenantId, scope);
    }
    return scope;
  }

  /**
   * Roll a scope's periods forward if a boundary has passed.
   *
   * Applied per scope, which is the fix for tenant counters accumulating forever: the
   * previous implementation reset only the global counters, so a tenant's daily total
   * grew from first request to process restart and its limit became a lifetime cap.
   */
  private rollover(scope: Scope): void {
    const dailyBoundary = this.currentDailyBoundary();
    const monthlyBoundary = this.currentMonthlyBoundary();

    if (scope.daily.periodStart < dailyBoundary) {
      scope.daily = this.createPeriod(dailyBoundary);
      scope.warned.daily = false;
    }

    if (scope.monthly.periodStart < monthlyBoundary) {
      scope.monthly = this.createPeriod(monthlyBoundary);
      scope.warned.monthly = false;
    }
  }

  private checkWarnings(
    scope: Scope,
    limits: { daily?: number; monthly?: number },
    tenantId: string | undefined,
  ): void {
    if (limits.daily !== undefined && !scope.warned.daily) {
      if (scope.daily.amount / limits.daily >= this.alertThreshold) {
        scope.warned.daily = true;
        this.emit('budget:warning', scope.daily.amount, limits.daily, 'daily', tenantId);
      }
    }

    if (limits.monthly !== undefined && !scope.warned.monthly) {
      if (scope.monthly.amount / limits.monthly >= this.alertThreshold) {
        scope.warned.monthly = true;
        this.emit('budget:warning', scope.monthly.amount, limits.monthly, 'monthly', tenantId);
      }
    }
  }

  private emit(
    event: keyof BudgetEvents,
    usage: number,
    limit: number,
    period: string,
    tenantId: string | undefined,
  ): void {
    this.emitter?.emit(event, {
      usage,
      limit,
      period,
      ...(tenantId !== undefined ? { tenantId } : {}),
    });
  }

  /** Midnight UTC today. */
  private currentDailyBoundary(): number {
    const now = new Date();
    return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  }

  /** First of the current month, midnight UTC. */
  private currentMonthlyBoundary(): number {
    const now = new Date();
    return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1);
  }

  private nextDailyBoundary(): number {
    const now = new Date();
    return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1);
  }

  /** Month + 1 with day 1 is safe: Date.UTC normalizes December into January. */
  private nextMonthlyBoundary(): number {
    const now = new Date();
    return Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1);
  }

  private createScope(): Scope {
    return {
      daily: this.createPeriod(this.currentDailyBoundary()),
      monthly: this.createPeriod(this.currentMonthlyBoundary()),
      warned: { daily: false, monthly: false },
    };
  }

  /**
   * periodStart is the BOUNDARY, not Date.now().
   *
   * Using now() means a scope created at 14:00 has a periodStart later than today's
   * boundary, so the comparison in rollover() never fires and the period never resets.
   */
  private createPeriod(boundary: number): PeriodUsage {
    return { amount: 0, periodStart: boundary, lastUpdated: Date.now() };
  }
}

export class BudgetExceededError extends Error {
  readonly code = 'BUDGET_EXCEEDED';

  constructor(
    message: string,
    readonly period: 'daily' | 'monthly',
    readonly usage: number,
    readonly limit: number,
    readonly tenantId?: string,
  ) {
    super(message);
    this.name = 'BudgetExceededError';
  }
}
