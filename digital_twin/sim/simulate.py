"""
Digital supply-chain twin: discrete-event simulation + Monte Carlo.

Models a single fulfillment node:
  - orders arrive as a Poisson process,
  - a warehouse with finite pick capacity (SimPy Resource) fulfills them,
  - replenishment from a supplier whose lead time is stochastic and can suffer
    an outage (a "what-if" disruption knob),
  - stockouts occur when on-hand hits zero.

Runs N Monte Carlo replications and reports the distribution of service level,
average order wait, and stockout hours. Optionally seeds initial on-hand from the
live lakehouse (Trino) so the twin starts from current reality.

Usage:
  python digital_twin/sim/simulate.py --runs 200 --days 14 --supplier-outage-prob 0.1
"""
import argparse
import statistics

import numpy as np
import simpy

RNG = np.random.default_rng(42)


def run_once(params: dict, seed: int) -> dict:
    rng = np.random.default_rng(seed)
    env = simpy.Environment()
    pickers = simpy.Resource(env, capacity=params["pickers"])
    state = {"on_hand": params["initial_on_hand"], "fulfilled": 0, "stockouts": 0,
             "waits": [], "stockout_time": 0.0}

    def replenish():
        while True:
            yield env.timeout(params["reorder_interval_h"])
            # supplier outage: occasionally lead time balloons
            if rng.random() < params["supplier_outage_prob"]:
                lead = params["lead_time_h"] * rng.uniform(3, 6)
            else:
                lead = max(1.0, rng.normal(params["lead_time_h"], params["lead_time_h"] * 0.2))
            yield env.timeout(lead)
            state["on_hand"] += params["reorder_qty"]

    def order(env, oid):
        t0 = env.now
        with pickers.request() as req:
            yield req
            if state["on_hand"] <= 0:
                state["stockouts"] += 1
                return
            yield env.timeout(rng.exponential(params["pick_time_h"]))
            state["on_hand"] -= 1
            state["fulfilled"] += 1
            state["waits"].append(env.now - t0)

    def arrivals(env):
        oid = 0
        while True:
            yield env.timeout(rng.exponential(1.0 / params["order_rate_per_h"]))
            oid += 1
            env.process(order(env, oid))

    def stock_monitor(env):
        while True:
            if state["on_hand"] <= 0:
                state["stockout_time"] += 1.0
            yield env.timeout(1.0)

    env.process(replenish())
    env.process(arrivals(env))
    env.process(stock_monitor(env))
    env.run(until=params["days"] * 24)

    total = state["fulfilled"] + state["stockouts"]
    return {
        "service_level": state["fulfilled"] / total if total else 1.0,
        "avg_wait_h": statistics.mean(state["waits"]) if state["waits"] else 0.0,
        "stockout_hours": state["stockout_time"],
    }


def monte_carlo(params: dict, runs: int) -> dict:
    results = [run_once(params, seed=int(RNG.integers(0, 2**31))) for _ in range(runs)]

    def summ(key):
        vals = [r[key] for r in results]
        return {
            "mean": round(statistics.mean(vals), 4),
            "p05": round(float(np.percentile(vals, 5)), 4),
            "p95": round(float(np.percentile(vals, 95)), 4),
        }

    return {k: summ(k) for k in ("service_level", "avg_wait_h", "stockout_hours")}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=200)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--pickers", type=int, default=5)
    ap.add_argument("--order-rate-per-h", type=float, default=8.0)
    ap.add_argument("--initial-on-hand", type=int, default=500)
    ap.add_argument("--reorder-qty", type=int, default=300)
    ap.add_argument("--reorder-interval-h", type=float, default=48.0)
    ap.add_argument("--lead-time-h", type=float, default=72.0)
    ap.add_argument("--pick-time-h", type=float, default=0.1)
    ap.add_argument("--supplier-outage-prob", type=float, default=0.05)
    args = ap.parse_args()

    params = {
        "days": args.days, "pickers": args.pickers,
        "order_rate_per_h": args.order_rate_per_h,
        "initial_on_hand": args.initial_on_hand, "reorder_qty": args.reorder_qty,
        "reorder_interval_h": args.reorder_interval_h, "lead_time_h": args.lead_time_h,
        "pick_time_h": args.pick_time_h, "supplier_outage_prob": args.supplier_outage_prob,
    }
    summary = monte_carlo(params, args.runs)
    print(f"Monte Carlo ({args.runs} runs, {args.days}d, outage_p={args.supplier_outage_prob}):")
    for metric, stats in summary.items():
        print(f"  {metric:16s} mean={stats['mean']:<10} p05={stats['p05']:<10} p95={stats['p95']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
