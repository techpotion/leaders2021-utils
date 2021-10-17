import pandas as pd
from geojson import Point, Feature
import json

from migrate_dataset import read_dataset_sheet_1

def gen_geojson():
    df = read_dataset_sheet_1()
    df = df[df['object_point_lat'].notna()]
    features = []
    for _, row in df.iterrows():
        try:
            new_point = Point((float(row['object_point_lng']), float(row['object_point_lat'])))
            feature = Feature(geometry=new_point)
            features.append(feature)
        except Exception as e:
            print('Ошибка!!: ', e)

    result = {
        "type": "FeatureCollection",
        "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },
        "features": features
    }

    return result

if __name__ == "__main__":
    output = gen_geojson()
    with open("export/heatmap.geojson", "w") as outfile:
        json.dump(output, outfile)