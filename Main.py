import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


class MeterReading:
    def __init__(self, timestamp, kwh):
        self.timestamp = timestamp
        self.kwh = kwh


class Building:
    def __init__(self, name):
        self.name = name
        self.meter_readings = []

    def add_reading(self, reading):
        self.meter_readings.append(reading)

    def calculate_total_consumption(self):
        return sum(r.kwh for r in self.meter_readings)

    def generate_report(self):
        total = self.calculate_total_consumption()
        return f"Building {self.name}: total consumption = {total:.2f} kWh"


class BuildingManager:
    def __init__(self):
        self.buildings = {}

    def get_or_create_building(self, name):
        if name not in self.buildings:
            self.buildings[name] = Building(name)
        return self.buildings[name]

    def load_from_dataframe(self, df):
        for _, row in df.iterrows():
            building_name = row.get("building", "Unknown")
            b = self.get_or_create_building(building_name)
            reading = MeterReading(row["timestamp"], row["kwh"])
            b.add_reading(reading)

    def campus_total_consumption(self):
        return sum(b.calculate_total_consumption() for b in self.buildings.values())

    def highest_consuming_building(self):
        if not self.buildings:
            return None
        return max(self.buildings.values(), key=lambda b: b.calculate_total_consumption())


def ingest_data(data_dir: Path):
    csv_files = list(data_dir.glob("*.csv"))
    frames = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
        except FileNotFoundError:
            continue
        except Exception:
            continue

        if "timestamp" not in df.columns or "kwh" not in df.columns:
            continue

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp", "kwh"])

        if "building" not in df.columns:
            df["building"] = file.stem.split("_")[0].capitalize()
        if "month" not in df.columns:
            df["month"] = file.stem.split("_")[-1]

        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["timestamp", "kwh", "building", "month"])

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values("timestamp")
    return combined


def calculate_daily_totals(df):
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    daily = df.resample("D")["kwh"].sum().reset_index()
    return daily


def calculate_weekly_aggregates(df):
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    weekly = df.resample("W")["kwh"].agg(["sum", "mean"]).reset_index()
    return weekly


def building_wise_summary(df):
    summary = df.groupby("building")["kwh"].agg(["mean", "min", "max", "sum"]).reset_index()
    summary = summary.rename(columns={"sum": "total"})
    return summary


def create_dashboard(daily, weekly, df, output_path: Path):
    fig, axes = plt.subplots(3, 1, figsize=(10, 12))

    axes[0].plot(daily["timestamp"], daily["kwh"], marker="o")
    axes[0].set_title("Daily Campus Consumption")
    axes[0].set_xlabel("Date")
    axes[0].set_ylabel("kWh")

    weekly_building = df.copy()
    weekly_building["timestamp"] = pd.to_datetime(weekly_building["timestamp"])
    weekly_building = weekly_building.set_index("timestamp")
    weekly_building = weekly_building.groupby("building")["kwh"].resample("W").mean().groupby("building").mean()
    axes[1].bar(weekly_building.index, weekly_building.values)
    axes[1].set_title("Average Weekly Usage per Building")
    axes[1].set_ylabel("kWh")

    df_peak = df.copy()
    df_peak["timestamp"] = pd.to_datetime(df_peak["timestamp"])
    df_peak["hour"] = df_peak["timestamp"].dt.hour
    axes[2].scatter(df_peak["hour"], df_peak["kwh"], c="tab:orange")
    axes[2].set_title("Peak-hour Consumption (All Buildings)")
    axes[2].set_xlabel("Hour of Day")
    axes[2].set_ylabel("kWh")

    plt.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)


def export_results(df, building_summary_df, output_dir: Path):
    cleaned_path = output_dir / "cleaned_energy_data.csv"
    summary_path = output_dir / "building_summary.csv"
    df.to_csv(cleaned_path, index=False)
    building_summary_df.to_csv(summary_path, index=False)


def generate_summary(df, building_manager: BuildingManager, output_dir: Path):
    if df.empty:
        text = "No data available to generate summary."
        (output_dir / "summary.txt").write_text(text)
        return

    total_campus = building_manager.campus_total_consumption()
    highest_building = building_manager.highest_consuming_building()
    highest_name = highest_building.name if highest_building else "N/A"
    highest_value = highest_building.calculate_total_consumption() if highest_building else 0

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    peak_row = df.loc[df["kwh"].idxmax()]
    peak_time = peak_row["timestamp"]
    peak_value = peak_row["kwh"]

    daily = calculate_daily_totals(df)
    weekly = calculate_weekly_aggregates(df)

    daily_trend = "increasing" if daily["kwh"].iloc[-1] > daily["kwh"].iloc[0] else "decreasing or stable"
    weekly_trend = "increasing" if weekly["sum"].iloc[-1] > weekly["sum"].iloc[0] else "decreasing or stable"

    lines = [
        f"Total campus consumption: {total_campus:.2f} kWh",
        f"Highest-consuming building: {highest_name} ({highest_value:.2f} kWh)",
        f"Peak load time: {peak_time} with {peak_value:.2f} kWh",
        f"Daily trend: {daily_trend}",
        f"Weekly trend: {weekly_trend}",
    ]

    summary_text = "\n".join(lines)
    (output_dir / "summary.txt").write_text(summary_text)
    print(summary_text)


def main():
    base_dir = Path(__file__).parent
    data_dir = base_dir / "data"
    output_dir = base_dir / "output"
    output_dir.mkdir(exist_ok=True)

    df_combined = ingest_data(data_dir)

    if df_combined.empty:
        print("No valid data found in data directory.")
        return

    daily_totals = calculate_daily_totals(df_combined)
    weekly_aggregates = calculate_weekly_aggregates(df_combined)
    building_summary_df = building_wise_summary(df_combined)

    manager = BuildingManager()
    manager.load_from_dataframe(df_combined)

    dashboard_path = output_dir / "dashboard.png"
    create_dashboard(daily_totals, weekly_aggregates, df_combined, dashboard_path)

    export_results(df_combined, building_summary_df, output_dir)
    generate_summary(df_combined, manager, output_dir)


if __name__ == "__main__":
    main()
