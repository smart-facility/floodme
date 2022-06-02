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

        return jsonify(result.all()[0][0])

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
    
        return jsonify(result.all()[0][0])


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
            (SELECT geom, id, ahd FROM sensors) AS t(geom, id, ahd)
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

        return jsonify(result.all()[0][0])

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
SELECT st_value((SELECT st_setsrid(rast, 4326) FROM hydraulics WHERE filename='20pct'), geom) AS flood_z, * FROM properties_with_aep WHERE current_aep = '20pct')
SELECT stamp, ground_z, floor_z, flood_z, flood_z - floor_z AS flood_depth, linked_sensor, geom FROM properties_with_levels WHERE flood_z - floor_z > -0.5
        ) AS t
        """), {"time": time})

    return jsonify(result.all()[0][0])

    


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