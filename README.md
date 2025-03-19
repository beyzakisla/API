# AQUAI API

## Description
A Flask-based API for managing and visualizing lake data.

## Usage
Instructions on how to use the project.

```bash
# Run the project
python app.py
```

## API Endpoints

### Get list of lakes
```
GET /api/v1/lakes
```
Returns a list of lake names.

### Get lake data
```
GET /api/v1/lakes/data
```
Parameters:
- `gol` (required): Name of the lake
- `start` (optional): Start date in YYYY-MM-DD format
- `end` (optional): End date in YYYY-MM-DD format

Returns data for the specified lake.

### Display lake graph
```
GET /api/v1/lakes/graph
```
Parameters:
- `gol` (optional): Name of the lake

Displays a graph of the lake's water level.