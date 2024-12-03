FROM python:3.9-slim

# Install netcat untuk pemeriksaan koneksi
RUN apt-get update && apt-get install -y netcat-openbsd

WORKDIR /app

# Salin seluruh konten aplikasi
COPY . .

# Install dependensi
RUN pip install --no-cache-dir -r requirements.txt

# Berikan izin eksekusi
RUN chmod +x ./entrypoint.sh

EXPOSE 8080

# Gunakan entrypoint untuk menunggu database
ENTRYPOINT ["./entrypoint.sh"]