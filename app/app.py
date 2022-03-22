from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from sqlalchemy import create_engine, text
import os

dbhost = os.environ.get("DBHOST")
pword = os.environ.get("DBPWORD")

app = Flask(__name__)

CORS(app)

# conn = psycopg2.connect(database="floodaware", user="postgres", host=dbhost, password=pword)
engine = create_engine(f"postgresql://postgres:{pword}@{dbhost}:5432/floodaware", echo=True, future=True)

@app.route("/")
def home():
    """a"""
    return render_template("index.html")

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


if (__name__ == "__main__"):
    app.run(host="0.0.0.0", port=5000)