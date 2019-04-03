# FlinkProject

Drivers, fleet owners, transport operations, insurance companies are stakeholders of
vehicle monitoring applications which need to have analytical reporting on the mobility patterns of their
vehicles, as well as real-time views in order to support quick and efficient decisions towards eco-friendly
moves, cost-effective maintenance of vehicles, improved navigation, safety and adaptive risk
management.

Vehicle sensors do continuously provide data, while on-the-move, which are processed in order to
provide valuable information to stakeholders. Applications identify speed violations, abnormal driver
behaviors, and/or other extraordinary vehicle machine conditions, produce statistics per
driver/vehicle/fleet/trip, correlate events with map positions and route, assist navigation, monitor fuel
consumptions, and perform many other reporting and alerting functions.

In this project we consider that each vehicle reports a position event every 30 seconds with the
following format: Time, VID, Spd, XWay, Lane, Dir, Seg, Pos

The goal of this project is to develop a Java program using Flink implementing the following functionality:

- Speed Radar: detect cars that overcome the speed limit of 90 mph

- Average Speed Control: detects cars with an average speed higher than 60 mph between segments 52 and 56 (both included) in both directions.

- Accident Reporter: detects stopped vehicles on any segment. A vehicle is stopped when it reports at least 4 consecutive events from the same position.

Notes:

- All metrics must take into account the direction field.

- A given vehicle could report more than 1 event for the same segment.

- Event time must be used for timestamping.

- Cars that do not complete the segment (52-56) are not taken into account by the average speed control. For example 52->54 or 55->56.

