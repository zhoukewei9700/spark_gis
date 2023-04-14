CREATE DATABASE mbtiles;

CREATE TABLE tiles (
                       zoom_level integer,
                       tile_column integer,
                       tile_row integer,
                       tile_data blob)ENGINE = InnoDB DEFAULT CHARSET = utf8;

INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data)
            VALUES(?,?,?,?);