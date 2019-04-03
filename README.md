# FlinkProject

The goal of this project is to develop a Java program using Flink implementing the following functionality:

- Speed Radar: detect cars that overcome the speed limit of 90 mph

- Average Speed Control: detects cars with an average speed higher than 60 mph between segments 52 and 56 (both included) in both directions.

- Accident Reporter: detects stopped vehicles on any segment. A vehicle is stopped when it reports at least 4 consecutive events from the same position.

Notes:

- All metrics must take into account the direction field.

- A given vehicle could report more than 1 event for the same segment.

- Event time must be used for timestamping.

- Cars that do not complete the segment (52-56) are not taken into account by the average speed control. For example 52->54 or 55->56.

