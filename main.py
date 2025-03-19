import json
import glob
import base64
from datetime import datetime
from io import BytesIO

from flask import Flask, request
from flask_cors import CORS
from flasgger import Swagger

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from collections import Counter

app = Flask(__name__)
app.url_map.strict_slashes = False 

CORS(app)

app.config['SWAGGER'] = {
    'title': 'AQUAI API'
}
swagger = Swagger(app)

DATABASE_URL = "mysql+pymysql://aquai:aquai123!!@4.233.145.179/aquai"

engine = create_engine(DATABASE_URL,pool_size=10,max_overflow=20,pool_recycle=3600,pool_timeout=30) 
Session = sessionmaker(bind=engine)

def connect_to_mysql():
  try:
    session = Session()
    print("Connected to MySQL")
    return session
  except SQLAlchemyError as e:
    print(f"Error connecting to MySQL: {e}")
    return None

mysql_conn = connect_to_mysql()

@app.before_request
def before_request():
    if not hasattr(request, 'session') or request.session is None:
        request.session = connect_to_mysql()  # Yeni bağlantı oluştur

@app.teardown_request
def teardown_request(exception):
  if hasattr(request, 'session'):
    try:
      request.session.close()
    except SQLAlchemyError as e:
      print(f"Error closing session: {e}")

@app.route("/<path:path>")
def home(path):
    return {"status": "200" ,"message": "Hello, World!"}
@app.route("/")
def home2():
    return {"status": "200" ,"message": "Hello, World!"}

@app.route("/api/v1/lakes", methods=["GET"])
def get_lakes():
    """
        Get list of lakes
        ---
        responses:
          200:
            description: A list of lake names
            schema:
              type: object
              properties:
                status:
                  type: string
                  example: "200"
                data:
                  type: array
                  items:
                    type: string
                  example: ["Lake1", "Lake2"]
          404:
            description: No data found
            schema:
              type: object
              properties:
                status:
                  type: string
                  example: "404"
                message:
                  type: string
                  example: "Veri bulunamadı"
    """
    try:
      result = request.session.execute(text("SELECT DISTINCT gol_adi FROM lakes"))
      data = result.fetchall()
      if not data:
        return {"status": "404", "message": "Veri bulunamadı"}
      lake_names = [item[0] for item in data]
      return {"status": "200", "data": lake_names}
    except SQLAlchemyError as e:
      return {"status": "500", "message": f"Veri tabanı hatası: {e}"}

@app.route("/api/v1/lakes/polygon", methods=["GET"])
def get_lake_polygon():
    """
      Get lake polygon data
      ---
      parameters:
        - name: gol
          in: query
          type: string
          required: true
          description: Name of the lake
        - name: start
          in: query
          type: string
          required: false
          description: Start date in YYYY-MM-DD format
        - name: end
          in: query
          type: string
          required: false
          description: End date in YYYY-MM-DD format
      responses:
        200:
          description: Lake polygon data
          schema:
            type: object
            properties:
            status:
              type: string
              example: "200"
            data:
              type: array
              items:
              type: object
              example: [{"date": "2023-01-01", "polygon": [[1, 2], [3, 4]]}]
        400:
          description: Invalid parameters
          schema:
            type: object
            properties:
            status:
              type: string
              example: "400"
            message:
              type: string
              example: "Eksik parametre: gol"
        404:
          description: No data found
          schema:
            type: object
            properties:
            status:
              type: string
              example: "404"
            message:
              type: string
              example: "Veri bulunamadı"
    """
    gol = request.args.get("gol")
    if not gol:
        return {"status": "400", "message": "Eksik parametre: gol"}

    start_date = request.args.get("start")
    end_date = request.args.get("end")

    for date_str in [start_date, end_date]:
      if date_str:
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return {"status": "400", "message": "Geçersiz tarih formatı. YYYY-MM-DD olmalı."}

    query = "SELECT tarih, polygon FROM lakes WHERE gol_adi = :gol AND polygon IS NOT NULL"
    params = {"gol": gol}

    if start_date:
      query += " AND tarih >= :start_date"
      params["start_date"] = start_date
    if end_date:
      query += " AND tarih <= :end_date"
      params["end_date"] = end_date

    query += " ORDER BY tarih DESC"
    query = text(query)

    result = request.session.execute(query, params)
    rows = result.fetchall()

    filtered_data = []
    for row in rows:
        filtered_data.append({
            "date": row[0],
            "polygon": json.loads(row[1])
        })

    if not filtered_data:
        return {"status": "404", "message": "Belirtilen tarih aralığında veri bulunamadı"}

    return {"status": "200", "data": filtered_data}

@app.route("/api/v1/lakes/data", methods=["GET"])
def get_lake_data():
    """
        Get lake data
        ---
        parameters:
          - name: gol
            in: query
            type: string
            required: true
            description: Name of the lake
          - name: start
            in: query
            type: string
            required: false
            description: Start date in YYYY-MM-DD format
          - name: end
            in: query
            type: string
            required: false
            description: End date in YYYY-MM-DD format
        responses:
          200:
            description: Lake data
            schema:
              type: object
              properties:
                status:
                  type: string
                  example: "200"
                data:
                  type: array
                  items:
                    type: object
                  example: [{"date": "Sat, 12 Nov 2016 00:00:00 GMT","pixel": "16364.04227"},{"date": "Sat, 19 Nov 2016 00:00:00 GMT","pixel": "16285.28962"}]
                data_count:
                  type: integer
                  example: 2
                lake_name:
                  type: string
                  example: "Lake1"
                data_start_date:
                  type: string
                  example: "2023-01-01"
                data_end_date:
                  type: string
                  example: "2023-01-02"
                start_image:
                  type: string
                  example: "base64 encoded image"
                start_image_date:
                  type: string
                  example: "2023-01-01"
                end_image:
                  type: string
                  example: "base64 encoded image"
                end_image_date:
                  type: string
                  example: "2023-01-02"
          400:
            description: Invalid parameters
            schema:
              type: object
              properties:
                status:
                  type: string
                  example: "400"
                message:
                  type: string
                  example: "Eksik parametre: gol"
          404:
            description: No data found
            schema:
              type: object
              properties:
                status:
                  type: string
                  example: "404"
                message:
                  type: string
                  example: "Veri bulunamadı"
    """

    gol = request.args.get("gol")
    if not gol:
        return {"status": "400", "message": "Eksik parametre: gol"}

    imagetype = request.args.get("itype")
    if imagetype and imagetype not in ["raw", "border"]:
        return {"status": "400", "message": "Geçersiz parametre: itype. raw ya da border olmalı."}
    
    filterd = {
        "start_date": request.args.get("start"),
        "end_date": request.args.get("end")
    }
    try:
        if filterd["start_date"]:
            datetime.strptime(filterd["start_date"], '%Y-%m-%d')
        if filterd["end_date"]:
            datetime.strptime(filterd["end_date"], '%Y-%m-%d')
    except ValueError:
        return {"status": "400", "message": "Geçersiz tarih formatı. YYYY-MM-DD olmalı."}

    query = "SELECT tarih, pixel FROM lakes WHERE gol_adi = :gol"
    params = {"gol": gol}

    if filterd["start_date"]:
      query += " AND tarih >= :start_date"
      params["start_date"] = filterd["start_date"]
    if filterd["end_date"]:
      query += " AND tarih <= :end_date"
      params["end_date"] = filterd["end_date"]

    query += " ORDER BY tarih DESC"
    query = text(query)

    result = request.session.execute(query, params)
    data = result.fetchall()

    if not data:
        return {"status": "404", "message": "Veri bulunamadı"}

    datay = []
    if filterd["start_date"] and filterd["end_date"]:
        if filterd["start_date"] > filterd["end_date"]:
            return {"status": "404", "message": "Başlangıç tarihi bitiş tarihinden büyük olamaz."}
        
    if len(data) == 1:
        datax = {}
        dated = data[0][0].strftime("%Y-%m-%d")
        if imagetype and imagetype == "raw":
            image_path = f"data/raw/{gol}/{dated}*.jpg"
        elif imagetype and imagetype == "border":
            image_path = f"data/border/{gol}/{dated}*.jpg"
        else:
            image_path = f"data/raw/{gol}/{dated}*.jpg"
        image_files = glob.glob(image_path)
        if image_files:
            with open(image_files[0], "rb") as image_file:
                encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
            datax["image"] = encoded_string
        datax["date"] = data[0][0]
        datax["pixel"] = data[0][1]
        datay.append(datax)
    else:

        for i in data:
            datax = {}
            datax["date"] = i[0]
            datax["pixel"] = i[1]
            datay.append(datax)

    first_date = data[0][0].strftime("%Y-%m-%d")
    last_date = data[-1][0].strftime("%Y-%m-%d")

    if imagetype and imagetype == "raw":
        first_image_path = f"data/raw/{gol}/{first_date}*.jpg"
        last_image_path = f"data/raw/{gol}/{last_date}*.jpg"
    elif imagetype and imagetype == "border":
        first_image_path = f"data/border/{gol}/{first_date}*.jpg"
        last_image_path = f"data/border/{gol}/{last_date}*.jpg"
    else:
        first_image_path = f"data/raw/{gol}/{first_date}*.jpg"
        last_image_path = f"data/raw/{gol}/{last_date}*.jpg"

    first_image_files = glob.glob(first_image_path)
    last_image_files = glob.glob(last_image_path)

    if not first_image_files:
        for record in data:
            first_date = record[0].strftime("%Y-%m-%d")
            if imagetype and imagetype == "raw":
                first_image_path = f"data/raw/{gol}/{first_date}*.jpg"
            elif imagetype and imagetype == "border":
                first_image_path = f"data/border/{gol}/{first_date}*.jpg"
            else:
                first_image_path = f"data/raw/{gol}/{first_date}*.jpg"
            first_image_files = glob.glob(first_image_path)
            if first_image_files:
                break

    if not last_image_files:
        for record in reversed(data):
            last_date = record[0].strftime("%Y-%m-%d")
            if imagetype and imagetype == "raw":
                last_image_path = f"data/raw/{gol}/{last_date}*.jpg"
            elif imagetype and imagetype == "border":
                last_image_path = f"data/border/{gol}/{last_date}*.jpg"
            else:
                last_image_path = f"data/raw/{gol}/{last_date}*.jpg"
            last_image_files = glob.glob(last_image_path)
            if last_image_files:
                break

    if first_image_files:
        with open(first_image_files[0], "rb") as image_file:
            first_encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
    else:
        first_encoded_string = None

    if last_image_files:
        with open(last_image_files[0], "rb") as image_file:
            last_encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
    else:
        last_encoded_string = None

    if len(data) == 1:
        return {"status": "200", "data": datay, "data_count": len(data), "lake_name": gol, "data_start_date": data[0][0], "data_end_date": data[0][0]}
    else:
        return {"status": "200", "data": datay, "data_count": len(data), "lake_name": gol, "data_start_date": data[0][0], "data_end_date": data[-1][0], "start_image": first_encoded_string, "start_image_date": first_date, "end_image": last_encoded_string, "end_image_date": last_date}

@app.route("/api/v1/lakes/graph", methods=["GET"])
def display_params():

    """
        Display lake water level graph
        ---
        parameters:
          - name: gol
            in: query
            type: string
            required: true
            description: Name of the lake
        responses:
          200:
            description: Lake water level graph
            schema:
              type: object
              properties:
                status:
                  type: string
                  example: "200"
                data:
                  type: string
                  example: "base64 encoded image"
          400:
            description: Invalid parameters
            schema:
              type: object
              properties:
                status:
                  type: string
                  example: "400"
                message:
                  type: string
                  example: "Eksik parametre: gol"
          404:
            description: No data found
            schema:
              type: object
              properties:
                status:
                  type: string
                  example: "404"
                message:
                  type: string
                  example: "Veri bulunamadı"
    """

    gol = request.args.get("gol") 

    if not gol:
        return {"status": "400", "message": "Eksik parametre: gol"}

    query = text("SELECT * FROM lakes WHERE gol_adi = :gol")
    params = {"gol": gol}

    result = request.session.execute(query, params)
    data = result.fetchall()

    if not data:
        return {"status": "404", "message": "Veri bulunamadı"}

    today = datetime.now()

    past_data = [(item[2], float(item[3])) for item in data if item[2] <= today]
    future_data = [(item[2], float(item[3])) for item in data if item[2] > today]

    plt.figure(figsize=(10, 6))

    if past_data:
        past_dates, past_levels = zip(*past_data)
        plt.plot(past_dates, past_levels, color='blue', linestyle='-', label='Bugünden Önce')

    if future_data:
        future_dates, future_levels = zip(*future_data)
        plt.plot(future_dates, future_levels, color='red', linestyle='-', label='Bugünden Sonra')

    plt.title(f"{gol} Su Seviyesi")
    plt.xlabel("Tarih")
    plt.ylabel("Su Seviyesi")
    plt.grid(True)
    plt.tight_layout()

    img = BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode('utf8')
    plt.close()

    return {"status": "200", "data": plot_url}
    
@app.route("/api/v1/lakes/heatmap", methods=["GET"])
def generate_heatmap():
    """
        Generate heatmap for a lake
        ---
        parameters:
          - name: gol
            in: query
            type: string
            required: true
            description: Name of the lake
          - name: start
            in: query
            type: string
            required: false
            description: Start date in YYYY-MM-DD format
          - name: end
            in: query
            type: string
            required: false
            description: End date in YYYY-MM-DD format
        responses:
          200:
            description: Heatmap image
            schema:
              type: object
              properties:
                status:
                  type: string
                  example: "200"
                data:
                  type: string
                  example: "base64 encoded image"
          400:
            description: Invalid parameters
            schema:
              type: object
              properties:
                status:
                  type: string
                  example: "400"
                message:
                  type: string
                  example: "Eksik parametre: gol"
          404:
            description: No data found
            schema:
              type: object
              properties:
                status:
                  type: string
                  example: "404"
                message:
                  type: string
                  example: "Veri bulunamadı"
    """
    gol = request.args.get("gol")
    if not gol:
        return {"status": "400", "message": "Eksik parametre: gol"}

    start_date = request.args.get("start")
    end_date = request.args.get("end")

    for date_str in [start_date, end_date]:
      if date_str:
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return {"status": "400", "message": "Geçersiz tarih formatı. YYYY-MM-DD olmalı."}

    query = "SELECT polygon, pixel FROM lakes WHERE gol_adi = :gol"
    params = {"gol": gol}

    if start_date:
      query += " AND tarih >= :start_date"
      params["start_date"] = start_date
    if end_date:
      query += " AND tarih <= :end_date"
      params["end_date"] = end_date

    query += " ORDER BY tarih DESC"
    query = text(query)

    result = request.session.execute(query, params)
    rows = result.fetchall()

    if not rows:
        return {"status": "404", "message": "Veri bulunamadı"}

    coord_counts = Counter()

    last_polygon = None
    max_y_global = float('-inf')

    for index, (polygon_json, _) in enumerate(rows):
        if not polygon_json:
            continue

        polygon_data = json.loads(polygon_json)
        coords = polygon_data["coordinates"][0]
        if index == 0:
            last_polygon = coords

        local_max_y = max(float(coord[1]) for coord in coords)
        if local_max_y > max_y_global:
            max_y_global = local_max_y

        for coord in coords:
            x_val = float(coord[0])
            y_val = float(coord[1])
            coord_counts[(x_val, y_val)] += 1

    if not last_polygon:
        return {"status": "404", "message": "Veri bulunamadı"}

    x_coords = []
    y_coords = []
    intensities = []
    for (x_val, y_val), count in coord_counts.items():
        x_coords.append(x_val)
        y_coords.append(max_y_global - y_val)
        intensities.append(count)

    if not x_coords or not y_coords or not intensities:
        return {"status": "400", "message": "Geçerli veriler bulunamadı"}

    plt.figure(figsize=(10, 6))

    last_x = [float(coord[0]) for coord in last_polygon]
    last_y = [max_y_global - float(coord[1]) for coord in last_polygon]
    if len(last_x) > 2 and len(last_y) > 2:  # Ensure the polygon is valid
        plt.fill(last_x, last_y, color='blue', alpha=0.5)

    try:
        plt.hexbin(x_coords, y_coords, C=intensities, gridsize=80, cmap='hot')
    except Exception as e:
        print(f"Error generating heatmap: {e}")
        return {"status": "500", "message": "Heatmap oluşturulurken bir hata oluştu"}
    plt.axis('off')
    plt.tight_layout()

    img = BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plt.close()

    # Return as a PNG response
    return app.response_class(img, mimetype='image/png')

    
if __name__ == "__main__":
    print("Starting AQUAI API")
    app.run(host='0.0.0.0', port=3000, debug=True, threaded=True)