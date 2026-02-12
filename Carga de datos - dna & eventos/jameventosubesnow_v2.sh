#!/usr/bin/sh

### BLOQUE 1: CONFIGURACIÓN DE ENTORNO ###
AWS_DEFAULT_REGION=us-east-1
export AWS_DEFAULT_REGION

# Variables de control de días (Hardcodeadas a 1).
# NOTA: En este script específico no parecen afectar el nombre de los archivos
# ya que los archivos de eventos tienen nombres estáticos.
ONE=1
TWO=1

# Captura de fecha para logs
dataset_date=`date`
TODAY=`date -d "$dataset_date" +%d-%b-%G-%T`
echo $TODAY

# Calcula una fecha de ayer (aunque no se usa en los comandos AWS de abajo).
TODAY2=`date -d "$dataset_date - $TWO days" +%Y-%m-%d`

# Asegura acceso a los binarios necesarios
export PATH=$PATH:/usr/local/bin

### BLOQUE 2: LIMPIEZA DE STAGING (S3 HOUSEKEEPING) ###
# Objetivo: Borrar los archivos que se procesaron ayer para evitar duplicados 
# o mezclas si la copia nueva falla.
# Se eliminan de la raíz 'jamaica/'.
aws s3 rm s3://filereplicatecwpstratainput/jamaica/jamaica_evtopup_snow.csv
aws s3 rm s3://filereplicatecwpstratainput/jamaica/jamaica_evadjustment_snow.csv
aws s3 rm s3://filereplicatecwpstratainput/jamaica/jamaica_evlowbalance_snow.csv

# Pausa técnica para consistencia de S3
sleep 10



### BLOQUE 3: MOVIMIENTO DE ARCHIVOS (PROMOCIÓN A PROCESAMIENTO) ###
# Objetivo: Copiar los archivos "frescos" desde la subcarpeta '/eventos/' 
# hacia la ruta raíz donde Snowflake los espera.
# RIESGO DETECTADO: Los nombres son estáticos. Si el proceso anterior no actualizó
# la carpeta '/eventos/', este script volverá a cargar los datos de ayer sin dar error.
aws s3 cp s3://filereplicatecwpstratainput/jamaica/eventos/jamaica_evtopup_snow.csv s3://filereplicatecwpstratainput/jamaica/
aws s3 cp s3://filereplicatecwpstratainput/jamaica/eventos/jamaica_evadjustment_snow.csv s3://filereplicatecwpstratainput/jamaica/
aws s3 cp s3://filereplicatecwpstratainput/jamaica/eventos/jamaica_evlowbalance_snow.csv s3://filereplicatecwpstratainput/jamaica/

sleep 20

### BLOQUE 4: EJECUCIÓN EN SNOWFLAKE (CARGA BULK) ###
# Configuración de credenciales (Texto plano - A corregir en automatización).
export SNOWSQL_ACCOUNT="dab61162.us-east-1"
export SNOWSQL_USER="MODELOS_CWC_LLA"
export SNOWSQL_PWD='j$AdOh018p24M4c#'
export SNOWSQL_DATABASE="CARIBBEAN"
export SNOWSQL_SCHEMA="CWC_DS"
export SNOWSQL_ROLE="ROL_MODELOS_CWC"
export SNOWSQL_WAREHOUSE="ANALYSIS_CWC"

# Ejecución secuencial de los scripts de carga.
# 1. Carga Recargas (Topups)
snowsql -f /librerias/sas94/strata/jamaica/script/jameventos_v2.sql

# 2. Carga Ajustes (Adjustments)
snowsql -f /librerias/sas94/strata/jamaica/script/jamadjustment_v2.sql

# 3. Carga Saldo Bajo (Low Balance)
snowsql -f /librerias/sas94/strata/jamaica/script/jamlowbalance_v2.sql

### BLOQUE 5: AUDITORÍA (ACTUALMENTE DESACTIVADA) ###
# Definición de metadatos para el API Gateway.
DATO1="jameventos - jamadjustment - jamlowbalance"
DATO2="jamaica"
DATO3="eventos jamaica"
DATO4="1"

# NOTA: La línea del CURL está comentada (#).
# Esto significa que este paso NO está reportando éxito/fallo al dashboard de monitoreo actualmente.
#curl --location --request POST 'https://pbxdv08o42.execute-api.us-east-1.amazonaws.com/dev' --header 'Accept: application/json' --header 'Content-Type: application/json' --data '{"version": 1,"correlationId": "1","clientTimeZone": "America/Panama","inputs": {"orderId": "1","channelId": "JAM DASH","customerId": "1","productType": "'"$DATO1"'", "productName": "'"$DATO2"'","productDetail": "'"$DATO3"'","purchaseTime": "'"$TODAY"'","purchaseAmount": "'"$DATO4"'","purchaseExpiryDate": "'"$TODAY"'","accountBalance": "0", "sourceSystemName": "'"$TABLA"'","createTimestamp": "'"$TODAY"'","runTimeStamp":"'"$TODAY"'","runId": "50b2d44c-96c8-497e-a27a-65387fcd7531"}}'

exit
