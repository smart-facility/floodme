from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from sqlalchemy import create_engine, text
import os, json

dbhost = os.environ.get("DBHOST")
pword = os.environ.get("DBPWORD")

app = Flask(__name__,
            static_url_path="",
            static_folder="static")

CORS(app)

# conn = psycopg2.connect(database="floodaware", user="postgres", host=dbhost, password=pword)
engine = create_engine(f"postgresql://postgres:{pword}@{dbhost}:5432/floodaware", echo=False, future=True)

@app.route("/")
def home():
    """a"""
    islive = False
    startdate = 20200207
    enddate = 20200208
    daysback = 1
    if "live" in list(request.args):
        islive = bool(request.args["live"])
    if "daysback" in list(request.args):
        daysback = str(request.args["daysback"])
    if "startdate" in list(request.args):
        startdate = str(request.args["startdate"])
    if "enddate" in list(request.args):
        enddate = str(request.args["enddate"])
    
    return render_template("index.html", flaskislive=int(islive), flaskdaysback=daysback, flaskstartdate=startdate, flaskenddate=enddate)

@app.route("/mb")
def map():
    """a"""
    return render_template("map.html")

@app.route("/api/catchment")
def catchment():
    """a"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(st_asgeojson(t.*)::json)
            )
            FROM
            (SELECT geom, id FROM catchment) AS t(geom, id)
        """))
        return jsonify(result.all()[0][0])

@app.route("/api/rainfall")
def rainfall():
    """a"""
    startdate = str(request.args["startdate"])
    enddate = str(request.args["enddate"])

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(st_asgeojson(t.*)::json)
            )
            FROM
            (SELECT st_forcepolygoncw(geom) AS geom, val, stamp FROM rainfall_raster,
            st_dumpaspolygons(
                st_clip(rast,
                    (SELECT st_expand(st_envelope(st_collect(geom)), 0.01) FROM catchment)
                )
            )
            WHERE val > 0 AND stamp BETWEEN :start AND :end) AS t(geom, val, stamp)
        """), {"start": startdate, "end": enddate})

        results = result.all()[0][0]
        if (results["features"] == None):
            results["features"] = []
        return jsonify(results)

@app.route("/api/rainfall/avg")
def rainfall_avg():
    """a"""
    startdate = str(request.args["startdate"])
    enddate = str(request.args["enddate"])
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(t.*)
            )
            FROM
            (SELECT
            (st_summarystats(
                st_clip(rast,
                    (SELECT st_expand(st_envelope(st_collect(geom)), 0.01) FROM catchment)
                )
            )).mean as avg, stamp FROM rainfall_raster            
            WHERE stamp BETWEEN :start AND :end) AS t(avg, stamp)
        """), {"start": startdate, "end": enddate})
    
        results = result.all()[0][0]
        if (results["features"] == None):
            results["features"] = []
        return jsonify(results)


@app.route("/api/sensors")
def sensors():
    """a"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(st_asgeojson(t.*)::json)
            )
            FROM
            (SELECT geom, id, ahd FROM sensors WHERE id NOT IN (10, 21)) AS t(geom, id, ahd)
        """))

        return jsonify(result.all()[0][0])

@app.route("/api/sensors/data")
def sensors_data():
    """a"""
    startdate = str(request.args["startdate"])
    enddate = str(request.args["enddate"])

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(t.*)
            )
            FROM
            (SELECT id, level, stamp FROM sensor_levels WHERE stamp BETWEEN :start AND :end ORDER BY stamp ASC) as t(id, level, stamp)
        """),{"start": startdate, "end": enddate})

        results = result.all()[0][0]
        if (results["features"] == None):
            results["features"] = []
        return jsonify(results)

@app.route("/api/transects")
def transects():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(st_asgeojson(t.*)::json)
            )
            FROM
            (SELECT gid AS id, geom, name, regr, catchment FROM transects) AS t
        """))

        return jsonify(result.all()[0][0])

@app.route("/api/transects/data")
def transects_data():
    startdate = str(request.args["startdate"])
    enddate = str(request.args["enddate"])
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(t.*)
            )
            FROM
            (SELECT gid AS id, regr[1]*flow^0 + regr[2]*flow^1 + regr[3]*flow^2 + regr[4]*flow^3 AS level, timestep AS stamp FROM experiment_data JOIN transects using(catchment) WHERE index = 9000001 AND timestep BETWEEN :start AND :end ORDER BY timestep ASC) as t(id, level, stamp)
        """),{"start": startdate, "end": enddate})

        results = result.all()[0][0]
        if (results["features"] == None):
            results["features"] = []
        return jsonify(results)



@app.route("/api/hotspots")
def hotspot():
    """a"""
    with engine.connect() as conn:
        result = conn.execute(text("""
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(st_asgeojson(t.*)::json)
        )
        FROM
        (SELECT st_force2d(geom) AS geom, current_flood_z - floor_z AS flood_depth FROM properties WHERE floor_z - 0.5 < current_flood_z) AS t
        """))

    return jsonify(result.all()[0][0])

@app.route("/api/hotspots/at")
def hotspots_at():
    """a"""
    time = str(request.args["time"])
    with engine.connect() as conn:
        result = conn.execute(text("""
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(st_asgeojson(t.*)::json)
        )
        FROM
        (
            WITH
time_selection AS (SELECT (:time)::timestamp AS time),
latest AS (SELECT DISTINCT ON (id) stamp, id, level FROM sensor_levels WHERE stamp BETWEEN (SELECT time - INTERVAL '30min' FROM time_selection) AND (SELECT time FROM time_selection) ORDER BY id, stamp DESC),
joined AS (SELECT stamp, id, name, ahd-level::float/1000 AS level, ahd, aep, geom FROM latest JOIN sensors using(id)),
current_aeps AS
(SELECT
*,
CASE
	WHEN level > aep[6] THEN 'PMF'
	WHEN level > aep[5] THEN '1pct'
	WHEN level > aep[4] THEN '2pct'
	WHEN level > aep[3] THEN '5pct'
	WHEN level > aep[2] THEN '10pct'
	WHEN level >= aep[1] THEN '20pct'
END
AS current_aep
FROM joined),
properties_with_aep AS (SELECT stamp, gid, current_aep, floor_z, ground_z, linked_sensor, st_force2d(properties.geom) AS geom FROM properties JOIN current_aeps ON linked_sensor = id),
properties_with_levels AS
(SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='PMF'), geom) AS flood_z, * FROM properties_with_aep WHERE current_aep = 'PMF'
UNION
SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='1pct'), geom) AS flood_z, * FROM properties_with_aep WHERE current_aep = '1pct'
UNION
SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='2pct'), geom) AS flood_z, * FROM properties_with_aep WHERE current_aep = '2pct'
UNION
SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='5pct'), geom) AS flood_z, * FROM properties_with_aep WHERE current_aep = '5pct'
UNION
SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='10pct'), geom) AS flood_z, * FROM properties_with_aep WHERE current_aep = '10pct'
UNION
SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='20pct'), geom) AS flood_z, * FROM properties_with_aep WHERE current_aep = '20pct'
UNION
SELECT null AS flood_z, * FROM properties_with_aep WHERE current_aep IS null)
SELECT stamp, ground_z, floor_z, COALESCE(flood_z, -20) AS flood_z, COALESCE(flood_z - floor_z, -20) AS flood_depth, linked_sensor, geom FROM properties_with_levels WHERE geom IS NOT NULL--WHERE flood_z - floor_z > -0.5
        ) AS t
        """), {"time": time})

    results = result.all()[0][0]
    if (results["features"] == None):
        results["features"] = []
    return jsonify(results)

@app.route("/api/infrastructure")
def infrastructure():
    """a"""
    time = str(request.args["time"])
    with engine.connect() as conn:
        result = conn.execute(text(
            """
            SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(st_asgeojson(t.*)::json)
        )
        FROM
        (
WITH
time_selection AS (SELECT (:time)::timestamp AS time),
latest AS (SELECT DISTINCT ON (id) stamp, id, level FROM sensor_levels WHERE stamp BETWEEN (SELECT time - INTERVAL '30min' FROM time_selection) AND (SELECT time FROM time_selection) ORDER BY id, stamp DESC),
joined AS (SELECT stamp, id, name, ahd-level::float/1000 AS level, ahd, aep, geom FROM latest JOIN sensors using(id)),
linked_sensors AS (SELECT * FROM (VALUES (1,12), (2,12), (4,12), (3,13), (7,13), (5,6), (6,6), (8,6), (35,6), (36,6), (10,14), (12,14), (15,14), (11,4), (9,11), (13,11), (33,11), (34,11), (44,11), (14,2), (23,2), (38,2), (16,39), (17,39), (18,39), (21,39), (32,41), (42,41), (24,38), (26,38), (40,38), (19,5), (20,5), (25,5), (43,5), (37,19), (29,9), (22,10), (27,10), (28,10), (47,10), (31,16), (39,16), (41,16), (45,16), (30,37), (46,37)) AS mapping (catchment, sensor)),
current_aeps AS
(SELECT
*,
CASE
	WHEN 2*level > aep[6] THEN 'PMF'
	WHEN level > aep[5] THEN '1pct'
	WHEN level > aep[4] THEN '2pct'
	WHEN level > aep[3] THEN '5pct'
	WHEN level > aep[2] THEN '10pct'
	WHEN level >= aep[1] THEN '20pct'
END
AS current_aep
FROM joined),
road AS (SELECT road_points.geom, road_points.catchment_id, linked_sensors.sensor, st_value((SELECT st_setsrid(rast, 4326) FROM dem), road_points.geom) AS ground_z FROM road_points JOIN linked_sensors ON road_points.catchment_id = linked_sensors.catchment),
road_aep AS (SELECT road.geom, current_aeps.current_aep, road.ground_z, sensor, stamp FROM road JOIN current_aeps ON road.sensor = current_aeps.id),
points_with_levels AS
(SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='20pct'), geom) AS flood_z, * FROM road_aep WHERE current_aep = '20pct'
UNION
 SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='10pct'), geom) AS flood_z, * FROM road_aep WHERE current_aep = '10pct'
 UNION
 SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='5pct'), geom) AS flood_z, * FROM road_aep WHERE current_aep = '5pct'
 UNION
 SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='2pct'), geom) AS flood_z, * FROM road_aep WHERE current_aep = '2pct'
 UNION
 SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='1pct'), geom) AS flood_z, * FROM road_aep WHERE current_aep = '1pct'
 UNION
 SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='PMF'), geom) AS flood_z, * FROM road_aep WHERE current_aep = 'PMF'
 UNION
SELECT null AS flood_z, * FROM road_aep WHERE current_aep IS null)
SELECT *, flood_z - ground_z AS flood_depth, sensor FROM points_with_levels WHERE flood_z > ground_z) AS t
"""
        ), {"time": time})
        results = result.all()[0][0]
        if (results["features"] == None):
            results["features"] = []
    return jsonify(results)


    


@app.route("/api/hotspots/dummy")
def hotty():
    """a"""
    sensors = tuple(json.loads(request.args["sensors"]))
    level = request.args["level"]

    with engine.connect() as conn:
        result = conn.execute(text("""
            WITH flood_areas AS (SELECT id, AHD, st_buffer(geom, 0.003) AS geom FROM sensors WHERE id IN :sensors),
            props AS (SELECT bc_id, ground_level, floor_level - ground_level AS floor_height, geom FROM properties)
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(st_asgeojson(t.*)::json)
            )
            FROM
            (SELECT bc_id, ground_level, geom FROM props WHERE st_contains((SELECT st_collect(geom) FROM flood_areas), geom) AND floor_height < :level) AS t
        """),{"sensors": sensors, "level": level})

    properties = result.all()[0][0]

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(st_asgeojson(t.*)::json)
            )
            FROM
            (SELECT id, AHD, st_buffer(geom, 0.003) AS geom FROM sensors WHERE id IN :sensors) AS t
        """), {"sensors": sensors})

    hotspots = result.all()[0][0]

    return jsonify({"properties": properties, "hotspots": hotspots})


if (__name__ == "__main__"):
    app.run(host="0.0.0.0", port=5000)