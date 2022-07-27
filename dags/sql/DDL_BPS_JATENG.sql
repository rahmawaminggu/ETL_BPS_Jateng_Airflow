use NBA_External_Analytics;

CREATE TABLE bps_dim_gender(
	id_gender nvarchar(10),
	desk_gender nvarchar(255)
);

CREATE TABLE bps_dim_kota_kab(
	id_kota_kab nvarchar(10),
	jenis_kota_kab nvarchar(255),
	desk_kota_kab nvarchar(255)
);

CREATE TABLE bps_dim_tahun(
	id_tahun nvarchar(10),
	desk_tahun nvarchar(255)
);

CREATE TABLE bps_fact_jumlah_penduduk(
	id nvarchar(50),
	jumlah_penduduk int,
	kota_kab nvarchar(10),
	jenis_kelamin nvarchar(10),
	tahun nvarchar(10)
)