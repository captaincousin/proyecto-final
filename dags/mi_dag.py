from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv
import requests
import logging
import psutil

load_dotenv()  # Cargar las variables de entorno desde el archivo .env

default_args = {
    'start_date': datetime(2023, 8, 5),
}

def check_internet_connection():
    try:
        response = requests.get("https://www.google.com", timeout=5)
        if response.status_code == 200:
            logging.info("Internet connection is available.")
        else:
            logging.warning("Internet connection is not available.")
    except requests.ConnectionError:
        logging.warning("Internet connection is not available.")

def enviar_correo():
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = os.getenv("SMTP_PORT")
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    to = 'cristhianperez147@gmail.com'  # Reemplazar con el destinatario real
    subject = 'Airflow: Se insertó correctamente los datos en RedShift'
    body = 'Los datos se descargaron e insertaron correctamente en la base de datos, que tengas Buen día.'
    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = to
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        message = msg.as_string()
        server.sendmail(smtp_user, to, message)
        server.quit()
        logging.info("Correo enviado exitosamente.")
    except Exception as e:
        logging.error(f"Error al enviar el correo: {str(e)}")

def check_cpu_usage():
    cpu_percent = psutil.cpu_percent(interval=1)
    logging.info(f"CPU Usage: {cpu_percent}%")
    if cpu_percent >= 90:
        send_cpu_limit_email()

def send_cpu_limit_email():
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = os.getenv("SMTP_PORT")
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    to = 'cristhianperez147@gmail.com'  # Reemplazar con el destinatario real
    subject = 'Uso límite del CPU'
    body = 'El uso del CPU ha alcanzado el 90% o más. Revisar el proceso de inserción.'

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = to
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        message = msg.as_string()
        server.sendmail(smtp_user, to, message)
        server.quit()
        logging.info("Correo de uso límite del CPU enviado exitosamente.")
    except Exception as e:
        logging.error(f"Error al enviar el correo: {str(e)}")


def check_disk_space():
    disk_percent = psutil.disk_usage('/').percent
    logging.info(f"Disk Space Usage: {disk_percent}%")
    if disk_percent >= 90:
        send_disk_limit_email()

def send_disk_limit_email():
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = os.getenv("SMTP_PORT")
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    to = 'cristhianperez147@gmail.com'  # Reemplazar con el destinatario real
    subject = 'Uso limite del espacio en disco'
    body = 'El uso del espacio en disco ha alcanzado el 90%, o más. Revisar el espacio disponible y liberar espacio si es necesario.'

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = to
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        message = msg.as_string()
        server.sendmail(smtp_user, to, message)
        server.quit()
        logging.info("Correo de uso límite del espacio en disco enviado exitosamente.")
    except Exception as e:
        logging.error(f"Error al enviar el correo: {str(e)}")
    

with DAG('mi_dag_complejo', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    # Tarea de inserción de datos en la base de datos
    tarea_docker = DockerOperator(
        task_id='ejecutar_docker',
        image='docker_image_task',
        api_version='auto',
        auto_remove=True,
        docker_url='TCP://docker-socket-proxy:2375',
        network_mode='bridge',
        dag=dag
    )

    # Tareas de monitoreo en paralelo
    check_internet_connection_task = PythonOperator(
        task_id='check_internet_connection',
        python_callable=check_internet_connection,
        dag=dag
    )

    check_cpu_usage_task = PythonOperator(
        task_id='check_cpu_usage',
        python_callable=check_cpu_usage,
        dag=dag
    )

    # Tarea de envío de correo al finalizar la inserción de datos
    enviar_correo_task = PythonOperator(
        task_id='enviar_correo_task',
        python_callable=enviar_correo,
        dag=dag
    )

    check_disk_space_task = PythonOperator(
        task_id='check_disk_space',
        python_callable=check_disk_space,
        dag=dag
    )

    # Definir las dependencias entre tareas
    check_disk_space_task >> check_internet_connection_task >> [tarea_docker, check_cpu_usage_task] >> enviar_correo_task
