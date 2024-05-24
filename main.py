import time
import fastf1 as ff1
import rerun as rr
import numpy as np

rr.init("F1", spawn=True)


year = 2024
wknd = 3
ses = "R"
driver = "LEC"

session = ff1.get_session(year, wknd, ses)
weekend = session.event
session.load()
lap = session.laps.pick_driver(driver).pick_fastest()

x = lap.telemetry["X"]
y = lap.telemetry["Y"]
color = lap.telemetry["Speed"]
points = np.array([x, y]).T.reshape(-1, 2)

rr.log("track", rr.LineStrips2D(points, radii=20))

start_time = lap.telemetry["Date"].iloc[0]
last_time = start_time

rr.log("driver/speed", rr.SeriesLine())
for datum in lap.telemetry.iloc:
    cur_time = datum["Date"]
    since_start = (cur_time - start_time).total_seconds()
    rr.set_time_sequence("step", int(since_start * 1000))
    time.sleep((cur_time - last_time).total_seconds())
    last_time = cur_time
    rr.log("track", rr.LineStrips2D(points, radii=20))
    rr.log(
        "track/driver",
        rr.Points2D([datum["X"], datum["Y"]], colors=[(255, 125, 125)], radii=50),
    )
    rr.log(
        "driver/speed",
        rr.Scalar(datum["Speed"]),
        # rr.Points2D([(cur_time - start_time).total_seconds(), datum["Speed"]]),
    )
