# SpatialDataGen
## _A Scalable Spatial Stream Generator_



## Valid Geometries

| Geometry | Number of Points/Vertices | Number of Holes | Number of Geometries (for Multi Geometries only) | Geometry/MultiGeometry Generation Algorithm |
| ------ | ------ |------ | ------ |------ |
| Point | NA | NA | NA | NA |
| LineString | 3 ~ 360 | NA | NA | 0 (Arc) |
| LineString | 3 ~ 1000 | NA | NA | 1 (Vertical) |
| LineString | 3 ~ 1000 | NA | NA | 2 (Horizontal) |
| Polygon | 4 ~ 1000 | 0 ~ 4 | NA | 0 (Box) |
| Polygon | 4 ~ 20 | 0 | NA | 1 (Arc) |
| Polygon | 5 ~ 20 | 2 ~ 4 | NA | 1 (Arc) |
| MultiPoint | 3 ~ 1000 | NA | 2,3,4,6,8,10 | 0 (Box) |
| MultiPoint | 3 ~ 1000 | NA | 2 ~ 1000 | 1 (Horizontal) |
| MultiPoint | 3 ~ 1000 | NA | 2 ~ 1000 | 2 (Vertical) |
| MultiLineString | 3 ~ 1000 | NA | 2,3,4,6,8,10 | 0 (Box) |
| MultiLineString | 3 ~ 1000 | NA | 2 ~ 1000 | 1 (Horizontal) |
| MultiLineString | 3 ~ 1000 | NA | 2 ~ 1000 | 2 (Vertical) |
| MultiPolygon | 4 ~ 1000 | 0 ~ 4 | 2,3,4,6,8,10 | 0 (Box) |
| MultiPolygon | 4 ~ 20 | 0 | 2 ~ 1000 | 1 (Horizontal) |
| MultiPolygon | 4 ~ 20 | 2 ~ 4 | 2 ~ 1000 | 2 (Vertical) |


## License
MIT
