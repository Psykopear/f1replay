from datetime import datetime, timezone
from dataclasses import dataclass
import fastf1 as ff1
import rerun as rr
import numpy as np
import rerun.blueprint as rrb

from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.inputs import StatefulSourcePartition, FixedPartitionedSource
from bytewax.outputs import StatelessSinkPartition, DynamicSink


class DriverSource(StatefulSourcePartition):
    def __init__(self, driver, telemetry, track):
        self.driver = driver
        self.points = track
        self._next_awake = datetime.now(tz=timezone.utc)
        self._last_time = telemetry.iloc[0]["Date"]
        self._data = telemetry.iloc
        self._it = iter(self._data)

    def next_batch(self):
        datum = next(self._it)
        dt = datum["Date"]
        self._next_awake += dt - self._last_time
        self._last_time = dt
        return [(self.driver, datum)]

    def next_awake(self):
        return self._next_awake

    def snapshot(self):
        return None


class RaceInput(FixedPartitionedSource):
    def __init__(self, session, frequency="original"):
        self.session = session
        lap = session.laps.pick_fastest()
        x = lap.telemetry["X"]
        y = lap.telemetry["Y"]
        self.track = np.array([x, y]).T.reshape(-1, 2)
        self.frequency = frequency

    def list_parts(self):
        return list(self.session.drivers)

    def build_part(self, step_id, for_part, resume_state):
        driver = self.session.get_driver(for_part)
        print(f"Building input for {driver['Abbreviation']}")
        return DriverSource(
            driver,
            self.session.laps.pick_driver(for_part).get_telemetry(
                frequency=self.frequency
            ),
            self.track,
        )


class RerunPartition(StatelessSinkPartition):
    def __init__(self, track):
        self.track = track
        self.start_time = None

    def write_batch(self, items):
        if self.start_time is None and len(items) > 0:
            self.start_time = items[0][1]["Date"]

        for driver, datum in items:
            dt = datum["Date"]
            name = driver["Abbreviation"]
            team_color = driver["TeamColor"]

            since_start = int((dt - self.start_time).total_seconds() * 1000)
            rr.set_time_sequence(f"{name}", since_start)
            rr.log("/track", rr.LineStrips2D(self.track, radii=20, draw_order=-1.0))

            # Convert hex string to rgb 0-255
            color = tuple(int(team_color[i : i + 2], 16) for i in (0, 2, 4))

            point = [datum["X"], datum["Y"]]
            rr.log(f"/track/{name}", rr.Points2D(point, colors=[color], radii=60))

            rr.log(f"/telemetry/speed/{name}", rr.Scalar(datum["Speed"]))
            rr.log(f"/telemetry/rpm/{name}", rr.Scalar(datum["RPM"]))
            rr.log(f"/telemetry/gear/{name}", rr.Scalar(datum["nGear"]))
            rr.log(
                f"/telemetry/throttle-brake/{name}",
                rr.Scalar(datum["Throttle"]),
                rr.Scalar(datum["Brake"] * 100),
            )


class RerunSink(DynamicSink):
    def __init__(self, track):
        self.track = track

    def build(self, step_id, worker_index, worker_count):
        return RerunPartition(self.track)


# Init rerun
rr.init("F1RaceSim")
rr.connect()

corner = rrb.Corner2D.LeftBottom
speed = rrb.TimeSeriesView(name="Speed", origin="/telemetry/speed", plot_legend=corner)
rpm = rrb.TimeSeriesView(name="RPM", origin="/telemetry/rpm", plot_legend=corner)
throttle = rrb.TimeSeriesView(
    name="Throttle", origin="/telemetry/throttle-brake", plot_legend=corner
)
gear = rrb.TimeSeriesView(name="Gear", origin="/telemetry/gear", plot_legend=corner)

telemetry = rrb.Vertical(
    contents=[
        rrb.Horizontal(contents=[speed, rpm]),
        rrb.Horizontal(contents=[throttle, gear]),
    ],
)

track = rrb.Spatial2DView(name="track", origin="/track")

blueprint = rrb.Blueprint(
    rrb.Horizontal(contents=[track, telemetry]),
    rrb.BlueprintPanel(expanded=True),
    rrb.SelectionPanel(expanded=False),
    rrb.TimePanel(expanded=False),
)

rr.send_blueprint(blueprint)


# Dataflow
def replay_session(year=2024, wknd=1, ses="R"):
    session = ff1.get_session(year, wknd, ses)
    session.load()
    lap = session.laps.pick_fastest()
    x = lap.telemetry["X"]
    y = lap.telemetry["Y"]
    track = np.array([x, y]).T.reshape(-1, 2)

    flow = Dataflow("f1-race-simulation")
    inp = op.input("f1-session-input", flow, RaceInput(session, frequency=10))
    op.output("rerun-output", inp, RerunSink(track))
    return flow
