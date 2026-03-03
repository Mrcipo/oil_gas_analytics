CREATE TABLE IF NOT EXISTS dim_empresa (
  id_empresa BIGSERIAL PRIMARY KEY,
  nombre_empresa TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_geografia (
  id_geo BIGSERIAL PRIMARY KEY,
  cuenca TEXT NOT NULL,
  provincia TEXT NOT NULL,
  yacimiento TEXT NOT NULL,
  CONSTRAINT uq_dim_geografia UNIQUE (cuenca, provincia, yacimiento)
);

CREATE TABLE IF NOT EXISTS dim_pozo (
  id_pozo BIGSERIAL PRIMARY KEY,
  nombre_pozo TEXT NOT NULL,
  tipo_reservorio TEXT NOT NULL,
  profundidad NUMERIC(12, 2),
  CONSTRAINT uq_dim_pozo UNIQUE (nombre_pozo, tipo_reservorio, profundidad)
);

CREATE TABLE IF NOT EXISTS dim_tiempo (
  fecha DATE PRIMARY KEY,
  anio SMALLINT NOT NULL,
  mes SMALLINT NOT NULL,
  trimestre SMALLINT NOT NULL,
  semestre SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_produccion (
  id_fact BIGSERIAL PRIMARY KEY,
  fecha DATE NOT NULL,
  id_empresa BIGINT NOT NULL,
  id_geo BIGINT NOT NULL,
  id_pozo BIGINT NOT NULL,
  prod_pet NUMERIC(18, 4) NOT NULL DEFAULT 0,
  prod_gas NUMERIC(18, 4) NOT NULL DEFAULT 0,
  prod_agua NUMERIC(18, 4) NOT NULL DEFAULT 0,
  iny_pet NUMERIC(18, 4) NOT NULL DEFAULT 0,
  iny_gas NUMERIC(18, 4) NOT NULL DEFAULT 0,
  iny_agua NUMERIC(18, 4) NOT NULL DEFAULT 0,
  tef NUMERIC(18, 4) NOT NULL DEFAULT 0,
  CONSTRAINT fk_fact_tiempo FOREIGN KEY (fecha) REFERENCES dim_tiempo(fecha),
  CONSTRAINT fk_fact_empresa FOREIGN KEY (id_empresa) REFERENCES dim_empresa(id_empresa),
  CONSTRAINT fk_fact_geo FOREIGN KEY (id_geo) REFERENCES dim_geografia(id_geo),
  CONSTRAINT fk_fact_pozo FOREIGN KEY (id_pozo) REFERENCES dim_pozo(id_pozo),
  CONSTRAINT uq_fact_grano UNIQUE (fecha, id_empresa, id_geo, id_pozo)
);

CREATE INDEX IF NOT EXISTS idx_fact_empresa ON fact_produccion(id_empresa);
CREATE INDEX IF NOT EXISTS idx_fact_geo ON fact_produccion(id_geo);
CREATE INDEX IF NOT EXISTS idx_fact_pozo ON fact_produccion(id_pozo);
CREATE INDEX IF NOT EXISTS idx_fact_fecha ON fact_produccion(fecha);
