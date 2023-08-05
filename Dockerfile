# Usa la imagen oficial de Python
FROM python:3.9

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia el script Python al contenedor  
COPY dags/mi_script.py .
COPY .env .

# Instala las dependencias necesarias
COPY requirements.txt .
RUN pip install -r requirements.txt

# Ejecuta el script Python cuando se inicie el contenedor
CMD ["python", "mi_script.py"]
