from collections import deque
from datetime import timedelta

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

WIND_DIRECTIONS_CLOCKWISE = ("N", "NE", "E", "SE", "S", "SO", "O", "NO")
TEMPERATURE_RANGE_CELSIUS = (0, 110)
HUMIDITY_RANGE_PERCENT = (0, 100)

FIGURE_BACKGROUND = "#F0F9FF"
PANEL_BACKGROUND = "#FFFFFF"
PRIMARY_TEXT_COLOR = "#0F172A"
MUTED_TEXT_COLOR = "#64748B"
GRID_COLOR = "#E0F0F8"
BORDER_COLOR = "#CBD5E1"

TEMPERATURE_COLOR = "#E11D48"
HUMIDITY_COLOR = "#0284C7"
WIND_COLOR = "#0D9488"

MINIMUM_TIME_PADDING = timedelta(seconds=15)
SNAPSHOT_DPI = 200


class LiveWeatherPlotter:
    def __init__(self, station_title, max_visible_readings=120):
        self.station_title = station_title
        self.total_readings = 0
        self.timestamps = deque(maxlen=max_visible_readings)
        self.temperatures = deque(maxlen=max_visible_readings)
        self.humidities = deque(maxlen=max_visible_readings)
        self.wind_direction_positions = deque(maxlen=max_visible_readings)

        plt.ion()
        plt.rcParams.update({
            "font.size": 11,
            "axes.titlesize": 12,
            "axes.labelsize": 11,
            "text.color": PRIMARY_TEXT_COLOR,
            "axes.labelcolor": MUTED_TEXT_COLOR,
            "xtick.color": MUTED_TEXT_COLOR,
            "ytick.color": MUTED_TEXT_COLOR,
        })
        self.figure, (self.temperature_axis, self.humidity_axis, self.wind_axis) = plt.subplots(
            3, 1, sharex=True, figsize=(12, 9), layout="constrained", facecolor=FIGURE_BACKGROUND
        )
        self.temperature_line, = self.temperature_axis.plot(
            [], [], color=TEMPERATURE_COLOR, marker="o", markersize=4, linewidth=1.8
        )
        self.humidity_line, = self.humidity_axis.plot(
            [], [], color=HUMIDITY_COLOR, marker="s", markersize=4, linewidth=1.8
        )
        self.wind_points = self.wind_axis.scatter([], [], color=WIND_COLOR, marker="D", s=36, zorder=3)

        self._configure_axes()
        self.waiting_message = self.figure.text(
            0.5, 0.5, "Esperando datos del sensor...", ha="center", va="center",
            fontsize=14, color=MUTED_TEXT_COLOR,
        )
        self.figure.suptitle(station_title, fontsize=15, fontweight="bold")
        plt.show(block=False)

    def add_reading(self, reading, received_at):
        wind_direction = reading["wind_direction"]
        if wind_direction not in WIND_DIRECTIONS_CLOCKWISE:
            raise ValueError(f"Direccion de viento desconocida: {wind_direction!r}")

        self.timestamps.append(received_at)
        self.temperatures.append(reading["temperature"])
        self.humidities.append(reading["humidity"])
        self.wind_direction_positions.append(WIND_DIRECTIONS_CLOCKWISE.index(wind_direction))
        self.total_readings += 1

    def refresh(self):
        if not self.timestamps:
            return

        self.waiting_message.set_visible(False)
        time_positions = mdates.date2num(list(self.timestamps))

        self.temperature_line.set_data(time_positions, list(self.temperatures))
        self.humidity_line.set_data(time_positions, list(self.humidities))
        self.wind_points.set_offsets(list(zip(time_positions, self.wind_direction_positions)))

        self._update_time_window()
        self._update_titles_with_latest_values()
        self.figure.canvas.draw_idle()

    def keep_window_responsive(self, pause_seconds):
        plt.pause(pause_seconds)

    def is_window_open(self):
        return plt.fignum_exists(self.figure.number)

    def save_snapshot(self, file_path):
        self.figure.savefig(file_path, dpi=SNAPSHOT_DPI)

    def _configure_axes(self):
        self._style_axis(self.temperature_axis, "Temperatura", "Temperatura (°C)", TEMPERATURE_RANGE_CELSIUS)
        self._style_axis(self.humidity_axis, "Humedad relativa", "Humedad (%)", HUMIDITY_RANGE_PERCENT)
        self._style_axis(
            self.wind_axis, "Dirección del viento", "Dirección",
            (-0.5, len(WIND_DIRECTIONS_CLOCKWISE) - 0.5),
        )
        self.wind_axis.set_yticks(range(len(WIND_DIRECTIONS_CLOCKWISE)), WIND_DIRECTIONS_CLOCKWISE)
        self.wind_axis.set_xlabel("Hora de la lectura (HH:MM:SS)")
        self.wind_axis.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

    def _style_axis(self, axis, title, y_label, value_range):
        lower_limit, upper_limit = value_range
        edge_margin = (upper_limit - lower_limit) * 0.04
        axis.set_ylim(lower_limit - edge_margin, upper_limit + edge_margin)
        axis.set_title(title, loc="left", fontweight="bold")
        axis.set_ylabel(y_label)
        axis.grid(True, color=GRID_COLOR, linewidth=0.8)
        axis.set_axisbelow(True)
        axis.set_facecolor(PANEL_BACKGROUND)
        axis.spines[["top", "right"]].set_visible(False)
        axis.spines[["left", "bottom"]].set_color(BORDER_COLOR)

    def _update_time_window(self):
        oldest_timestamp, newest_timestamp = self.timestamps[0], self.timestamps[-1]
        time_padding = max((newest_timestamp - oldest_timestamp) * 0.05, MINIMUM_TIME_PADDING)
        self.wind_axis.set_xlim(
            mdates.date2num(oldest_timestamp - time_padding),
            mdates.date2num(newest_timestamp + time_padding),
        )

    def _update_titles_with_latest_values(self):
        latest_wind_direction = WIND_DIRECTIONS_CLOCKWISE[self.wind_direction_positions[-1]]
        self.temperature_axis.set_title(
            f"Temperatura · última lectura: {self.temperatures[-1]:.2f} °C", loc="left", fontweight="bold"
        )
        self.humidity_axis.set_title(
            f"Humedad relativa · última lectura: {self.humidities[-1]} %", loc="left", fontweight="bold"
        )
        self.wind_axis.set_title(
            f"Dirección del viento · última lectura: {latest_wind_direction}", loc="left", fontweight="bold"
        )
        self.figure.suptitle(
            f"{self.station_title} · {self.total_readings} lecturas recibidas", fontsize=15, fontweight="bold"
        )
