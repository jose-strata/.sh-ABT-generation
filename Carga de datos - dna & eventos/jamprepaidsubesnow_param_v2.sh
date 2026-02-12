#!/usr/bin/sh

### BLOQUE 1: CONFIGURACIÓN INICIAL Y PARÁMETROS ###
# Define la región de AWS para asegurar que los comandos s3 cp/rm busquen en el lugar correcto.
AWS_DEFAULT_REGION=us-east-1
export AWS_DEFAULT_REGION

# Captura el primer argumento que se le pase al script (ej: ./script.sh 5).
# $1 es una variable reservada del sistema para el primer argumento posicional.
ONE_param=$1

# Lógica de "Valor por Defecto":
# Si no se pasa ningún parámetro (la variable está vacía "-z"), asume 2 días de atraso.
# Esto es vital para ejecuciones automáticas diarias (Batch Processing).
if [ -z "$ONE_param" ]
then
    ONE=2  # Default: Cargar datos de hace 2 días (T-2)
else
    ONE=$ONE_param # Manual: Usar el número de días que diga el operador
fi

# Duplica la variable para uso interno (redundancia heredada).
TWO=$ONE

### BLOQUE 2: ARITMÉTICA DE FECHAS (CALCULO DE VARIABLES TEMPORALES) ###
# Captura la fecha y hora actual del sistema.
dataset_date=`date`

# Genera formatos de fecha para logs y auditoría (DD-MMM-YYYY-HH:MM:SS).
TODAY=`date -d "$dataset_date" +%d-%b-%G-%T`

# Calcula la fecha del archivo a borrar (Housekeeping).
# No se usa en la lógica de borrado directo más abajo, pero define una fecha base.
TODAY2=`date -d "$dataset_date - $TWO days" +%Y-%m-%d`

# Marca de tiempo para nombres de logs únicos.
dtstamp=$(date +%Y.%m.%d_%H.%M.%S)

# --- VARIABLES CRÍTICAS DEL PROCESO ---
# dt: Fecha objetivo del archivo ORIGEN (Raw Data). Ej: 20260202
dt=$(date --date="$TWO days ago" +%Y%m%d) 

aproc=$ONE

# aeli: Fecha de eliminación. Se calcula como (Días de proceso + 1).
# Si procesamos T-2, borramos T-3 para mantener la carpeta limpia.
aeli=$((ONE+1)) 

# quita: La cadena de texto de la fecha a eliminar (Ej: 20260201).
quita=`date -d "$dataset_date - $aeli days" +%Y%m%d`

# proce: La cadena de texto de la fecha a procesar/renombrar (Ej: 20260202).
proce=`date -d "$dataset_date - $aproc days" +%Y%m%d`


### BLOQUE 3: MODO DEBUG Y VALIDACIÓN HUMANA ###
# 'set -x' imprime en consola cada comando antes de ejecutarlo. Útil para logs detallados.
set -x
# Parar deb
#set +x

echo a_cargar $dt
echo "Se va a cargar el día $dt\nse va a eliminar el día $quita\nse va a procesar el día $proce\n"

# --- BLOQUEO MANUAL (KILL SWITCH) ---
# ⚠️ IMPORTANTE PARA AUTOMATIZACIÓN: 
# Este bloque 'while' detiene el script esperando un 'Si/No' del usuario.
# Debe ser ELIMINADO al migrar a Airflow, o el DAG dará Timeout.
while true; do
    read -p "¿Desea continuar? (s/n): " respuesta
    case $respuesta in
        [Ss]* ) echo "OK..."; break;;         # Si es S, rompe el ciclo y continua
        [Nn]* ) echo "Saliendo..."; exit 0;;  # Si es N, mata el proceso exitosamente
        * ) echo "Respondio: $respuesta. Por favor responda s/S o n/N.";;
    esac
done

echo "esta continuando con el script"
echo "esta procesando el día $proce"


### BLOQUE 4: GESTIÓN DE ARCHIVOS EN AWS S3 (ETL - EXTRACT) ###
# Asegura que el binario de AWS CLI esté en el PATH.
export PATH=$PATH:/usr/local/bin

# PASO 1: LIMPIEZA (Housekeeping)
# Elimina el archivo que ya fue procesado el día anterior.
# Evita costos de almacenamiento innecesarios en la capa de 'Input/Staging'.
aws s3 rm s3://filereplicatecwpstratainput/jamaica/dnaprepaidjam$quita.csv

# Pausa de seguridad: Espera 40 seg para garantizar consistencia eventual de S3.
sleep 40

# PASO 2: PREPARACIÓN (Renombrado/Copia)
# Copia el archivo crudo (prepaid_$dt.csv) a un nuevo nombre estandarizado (dnaprepaidjam$proce.csv).
# Snowflake espera este nombre específico en los scripts SQL siguientes.
aws s3 cp s3://filereplicatecwpstratainput/jamaica/prepaid_$dt.csv s3://filereplicatecwpstratainput/jamaica/dnaprepaidjam"$proce".csv

sleep 45

### BLOQUE 5: CARGA A SNOWFLAKE (ETL - LOAD) ###
# Exporta variables de entorno para que la herramienta 'snowsql' se autentique.
# ⚠️ RIESGO DE SEGURIDAD: La contraseña está en texto plano. 
# En Airflow usaremos 'Snowflake Connections' para ocultar esto.
export SNOWSQL_ACCOUNT="dab61162.us-east-1"
export SNOWSQL_USER="MODELOS_CWC_LLA"
export SNOWSQL_PWD='j$AdOh018p24M4c#'
export SNOWSQL_DATABASE="CARIBBEAN"
export SNOWSQL_SCHEMA="CWC_DS"
export SNOWSQL_ROLE="ROL_MODELOS_CWC"
export SNOWSQL_WAREHOUSE="ANALYSIS_CWC"



# Ejecuta los scripts SQL pasando la variable 'proc' (fecha) dinámicamente.
# Esto carga los datos desde el CSV en S3 hacia las tablas de Snowflake.
snowsql -f /librerias/sas94/strata/jamaica/script/jamprepaid_v2.sql --variable proc=$proce
snowsql -f /librerias/sas94/strata/jamaica/script/islasprepaid_v2.sql --variable proc=$proce

### BLOQUE 6: AUDITORÍA Y NOTIFICACIÓN (API CALL) ###
# Define metadatos para el registro de auditoría.
DATO1="dnaprepaidjam.csv"
DATO2="jamaica"
DATO3="jam_ds_campaign_dashboard_pre"
DATO4="1"

# Realiza una petición POST a un API Gateway.
# Envía un JSON confirmando que el proceso terminó, la hora de ejecución y el archivo procesado.
curl --location --request POST 'https://pbxdv08o42.execute-api.us-east-1.amazonaws.com/dev' --header 'Accept: application/json' --header 'Content-Type: application/json' --data '{"version": 1,"correlationId": "1","clientTimeZone": "America/Panama","inputs": {"orderId": "1","channelId": "JAM DASH","customerId": "1","productType": "'"$DATO1"'", "productName": "'"$DATO2"'","productDetail": "'"$DATO3"'","purchaseTime": "'"$TODAY"'","purchaseAmount": "'"$DATO4"'","purchaseExpiryDate": "'"$TODAY"'","accountBalance": "0", "sourceSystemName": "'"$TABLA"'","createTimestamp": "'"$TODAY"'","runTimeStamp":"'"$TODAY"'","runId": "50b2d44c-96c8-497e-a27a-65387fcd7531"}}'

# Finaliza el script exitosamente.
exit