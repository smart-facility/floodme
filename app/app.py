from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import psycopg2, os

dbhost = os.environ.get("DBHOST")
pword = os.environ.get("DBPWORD")

app = Flask(__name__)

CORS(app)

conn = psycopg2.connect(database="floodaware", user="postgres", host=dbhost, password=pword)


@app.route("/")
def home():
    """a"""
    return render_template("index.html")

@app.route("/api/catchment")
def catchment():
    """a"""
    curs = conn.cursor()
    curs.execute("""
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(st_asgeojson(t.*)::json)
        )
        FROM
        (SELECT geom, id FROM catchment) AS t(geom, id)
    """)
    return jsonify(curs.fetchone()[0])

@app.route("/api/rainfall")
def rainfall():
    """a"""
    startdate = str(request.args["startdate"])
    enddate = str(request.args["enddate"])
    curs = conn.cursor()
    curs.execute("""
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
        WHERE val > 0 AND stamp BETWEEN %(start)s AND %(end)s) AS t(geom, val, stamp)
    """, {"start": startdate, "end": enddate})
    return jsonify(curs.fetchone()[0])

@app.route("/api/sensors")
def sensors():
    """a"""
    curs = conn.cursor()
    curs.execute("""
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(st_asgeojson(t.*)::json)
        )
        FROM
        (SELECT geom, id, ahd FROM sensors) AS t(geom, id, ahd)
    """)
    return jsonify(curs.fetchone()[0])

@app.route("/api/sensors/data")
def sensors_data():
    """a"""
    startdate = str(request.args["startdate"])
    enddate = str(request.args["enddate"])
    curs = conn.cursor()
    curs.execute("""
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(t.*)
        )
        FROM
        (SELECT id, level, stamp FROM sensor_levels WHERE stamp BETWEEN %(start)s AND %(end)s) as t(id, level, stamp)"""
        , {"start": startdate, "end": enddate})
    return jsonify(curs.fetchone()[0])


if (__name__ == "__main__"):
    app.run(host="0.0.0.0", port=5000)