import time
import os
import json
import boto3
import asyncio
from fastapi import FastAPI, Request
from fpdf import FPDF
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Notifications Worker")
#test
ENV = os.getenv("APP_ENVIRONMENT", "local")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL") 
BUCKET_NAME = os.getenv("BUCKET_NAME")
API_PUBLIC_URL = os.getenv("API_PUBLIC_URL", "http://localhost:8002")

# Clientes AWS
sqs = boto3.client('sqs', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)
sns = boto3.client('sns', region_name=AWS_REGION) 
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)

TABLE_CUSTOMERS = dynamodb.Table(os.getenv("TABLE_CUSTOMERS", "Customers"))
TABLE_NOTES = dynamodb.Table(os.getenv("TABLE_NOTES", "SalesNotes"))
TABLE_ITEMS = dynamodb.Table(os.getenv("TABLE_ITEMS", "SalesNoteItems"))
TABLE_PRODUCTS = dynamodb.Table(os.getenv("TABLE_PRODUCTS", "Products"))

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 14)
        self.cell(0, 10, 'Nota de Venta', 0, 1, 'C')
        self.ln(5)

async def sqs_worker():
    print(f"👷 Worker iniciado. Escuchando: {SQS_QUEUE_URL}")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10 
            )

            if 'Messages' in response:
                for message in response['Messages']:
                    print(f" Mensaje recibido: {message['MessageId']}")
                    await process_message(message)
                    
                    sqs.delete_message(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
            else:
                await asyncio.sleep(1)
                
        except Exception as e:
            print(f" Error en worker: {e}")
            await asyncio.sleep(5)

async def process_message(sqs_message):
    try:
        body = json.loads(sqs_message['Body'])
        if 'Message' in body:
            sns_content = json.loads(body['Message'])
            note_id = sns_content.get('noteId')
        else:
            note_id = body.get('noteId')

        if not note_id:
            print("⚠️ No se encontró noteId en el mensaje")
            return

        print(f" Procesando Nota ID: {note_id}")
        
        note_data = TABLE_NOTES.get_item(Key={'ID': note_id})['Item']
        customer_data = TABLE_CUSTOMERS.get_item(Key={'ID': note_data['clienteId']})['Item']
        
 
        response_items = TABLE_ITEMS.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('noteId').eq(note_id)
        )
        note_items = response_items['Items']

        pdf = PDF()
        pdf.add_page()
        pdf.set_font('Arial', '', 12)
        pdf.cell(0, 10, f"Folio: {note_data['folio']}", 0, 1)
        pdf.cell(0, 10, f"Cliente: {customer_data['razonSocial']}", 0, 1)
        
        pdf.ln(5)
        for item in note_items:
            prod = TABLE_PRODUCTS.get_item(Key={'ID': item['productoId']}).get('Item', {})
            nom_prod = prod.get('nombre', 'Producto')
            pdf.cell(0, 10, f"- {item['cantidad']} x {nom_prod} (${item['importe']})", 0, 1)

        pdf_content = pdf.output()

        rfc = customer_data.get('rfc', 'GENERICO')
        folio = note_data.get('folio', 'SIN_FOLIO')
        object_key = f"{rfc}/{folio}.pdf"
        
        s3.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=bytes(pdf_content), ContentType='application/pdf')
        
        s3.put_object_tagging(
            Bucket=BUCKET_NAME, Key=object_key,
            Tagging={'TagSet': [
                {'Key': 'hora-envio', 'Value': datetime.utcnow().isoformat()},
                {'Key': 'nota-descargada', 'Value': 'false'},
                {'Key': 'veces-enviado', 'Value': '1'}
            ]}
        )
        
        print(f"✅ PDF subido a: {object_key}")

    except Exception as e:
        print(f" Error procesando mensaje: {e}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(sqs_worker())

@app.get("/")
def health_check():
    return {"status": "ok", "service": "notifications-worker"}