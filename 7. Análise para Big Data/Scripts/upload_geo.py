import json
import os
import subprocess

# Sobe o GeoJSON dos distritos para o HDFS (corre uma vez).
HDFS_DIR  = "hdfs://hadoop-master:39000/2250055/geo"
HDFS_FILE = HDFS_DIR + "/distritos.geojson"
LOCAL_TMP = "/tmp/distritos.geojson"

geojson = {
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "name": "Aveiro"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -8.768,
                40.932
              ],
              [
                -8.645,
                40.93
              ],
              [
                -8.529,
                40.805
              ],
              [
                -8.514,
                40.676
              ],
              [
                -8.629,
                40.574
              ],
              [
                -8.768,
                40.6
              ],
              [
                -8.768,
                40.932
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Beja"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -8.285,
                38.32
              ],
              [
                -7.9,
                38.32
              ],
              [
                -7.6,
                37.9
              ],
              [
                -7.9,
                37.6
              ],
              [
                -8.285,
                37.7
              ],
              [
                -8.285,
                38.32
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Braga"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -8.7,
                41.7
              ],
              [
                -8.2,
                41.7
              ],
              [
                -8.2,
                41.4
              ],
              [
                -8.7,
                41.4
              ],
              [
                -8.7,
                41.7
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Bragança"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -7.3,
                41.9
              ],
              [
                -6.5,
                41.9
              ],
              [
                -6.5,
                41.3
              ],
              [
                -7.3,
                41.3
              ],
              [
                -7.3,
                41.9
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Castelo Branco"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -8.1,
                40.3
              ],
              [
                -7.2,
                40.3
              ],
              [
                -7.2,
                39.6
              ],
              [
                -8.1,
                39.6
              ],
              [
                -8.1,
                40.3
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Coimbra"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -8.7,
                40.4
              ],
              [
                -8.1,
                40.4
              ],
              [
                -8.1,
                39.9
              ],
              [
                -8.7,
                39.9
              ],
              [
                -8.7,
                40.4
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Évora"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -8.2,
                38.9
              ],
              [
                -7.3,
                38.9
              ],
              [
                -7.3,
                38.3
              ],
              [
                -8.2,
                38.3
              ],
              [
                -8.2,
                38.9
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Faro"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -8.9,
                37.3
              ],
              [
                -7.4,
                37.3
              ],
              [
                -7.4,
                36.9
              ],
              [
                -8.9,
                36.9
              ],
              [
                -8.9,
                37.3
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Guarda"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -7.7,
                41.1
              ],
              [
                -6.7,
                41.1
              ],
              [
                -6.7,
                40.5
              ],
              [
                -7.7,
                40.5
              ],
              [
                -7.7,
                41.1
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Leiria"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -9.1,
                39.9
              ],
              [
                -8.5,
                39.9
              ],
              [
                -8.5,
                39.3
              ],
              [
                -9.1,
                39.3
              ],
              [
                -9.1,
                39.9
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Lisboa"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -9.5,
                39.1
              ],
              [
                -8.8,
                39.1
              ],
              [
                -8.8,
                38.6
              ],
              [
                -9.5,
                38.6
              ],
              [
                -9.5,
                39.1
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Portalegre"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -8.1,
                39.4
              ],
              [
                -7.2,
                39.4
              ],
              [
                -7.2,
                38.8
              ],
              [
                -8.1,
                38.8
              ],
              [
                -8.1,
                39.4
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Porto"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -8.8,
                41.4
              ],
              [
                -8.3,
                41.4
              ],
              [
                -8.3,
                41.0
              ],
              [
                -8.8,
                41.0
              ],
              [
                -8.8,
                41.4
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Santarém"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -8.9,
                39.6
              ],
              [
                -8.1,
                39.6
              ],
              [
                -8.1,
                38.9
              ],
              [
                -8.9,
                38.9
              ],
              [
                -8.9,
                39.6
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Setúbal"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -9.2,
                38.6
              ],
              [
                -8.4,
                38.6
              ],
              [
                -8.4,
                37.9
              ],
              [
                -9.2,
                37.9
              ],
              [
                -9.2,
                38.6
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Viana do Castelo"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -8.9,
                41.9
              ],
              [
                -8.4,
                41.9
              ],
              [
                -8.4,
                41.6
              ],
              [
                -8.9,
                41.6
              ],
              [
                -8.9,
                41.9
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Vila Real"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -7.9,
                41.6
              ],
              [
                -7.2,
                41.6
              ],
              [
                -7.2,
                41.1
              ],
              [
                -7.9,
                41.1
              ],
              [
                -7.9,
                41.6
              ]
            ]
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Viseu"
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                -8.3,
                41.0
              ],
              [
                -7.5,
                41.0
              ],
              [
                -7.5,
                40.5
              ],
              [
                -8.3,
                40.5
              ],
              [
                -8.3,
                41.0
              ]
            ]
          ]
        ]
      }
    }
  ]
}

# 1) escreve o GeoJSON num ficheiro local temporario
with open(LOCAL_TMP, "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False)

# 2) cria a pasta no HDFS e sobe o ficheiro (overwrite)
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_DIR], check=True)
subprocess.run(["hdfs", "dfs", "-put", "-f", LOCAL_TMP, HDFS_FILE], check=True)
os.remove(LOCAL_TMP)
print("GeoJSON guardado em", HDFS_FILE)
