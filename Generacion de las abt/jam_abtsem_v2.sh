#!/bin/bash

### BLOQUE 1: CÁLCULO DE FECHA DE CORTE ###
# Calcula la fecha del último domingo (Cierre de semana).
# Formato: YYYYMMDD (Ej: 20260208).
HOY=`date  -d "last Sunday" '+%Y%m%d'`

### BLOQUE 2: PREPARACIÓN DE PLANTILLAS ###
# SQLORI: Archivo plantilla con la lógica de negocio (contiene #DOMINGO#).
SQLORI=jam_gen_semanal_abt_v2.sql

# SQL: Archivo temporal donde se guarda el código final listo para ejecutar.
SQL=/tmp/jam_gen_semanal_abt_v2.${HOY}.sql
echo >> ${SQL}

# LOG: Archivo de registro de la ejecución.
LOG=/tmp/jam_gen_semanal_abt_v2.${HOY}.log
echo >> ${LOG}

# Reemplaza #DOMINGO# por la fecha real usando 'sed'
cat ${SQLORI}|sed -e "s/#DOMINGO#/${HOY}/g" >${SQL}
cat ${SQL} # Muestra el código generado para depuración

### BLOQUE 3: CONFIGURACIÓN DE CONEXIÓN (IMPORTANTE) ###
# Aquí se definen explícitamente los parámetros de conexión a Snowflake.
# ⚠️ SEGURIDAD: La contraseña está en texto plano. Esto debe corregirse en Airflow.
accountname="dab61162.us-east-1"  # ID de la cuenta Snowflake
username="MODELOS_CWC_LLA"        # Usuario de servicio
SNOWSQL_PWD='j$AdOh018p24M4c#'    # Contraseña
rolename="ROL_MODELOS_CWC"        # Rol con permisos de escritura
warehousename="ANALYSIS_CWC"      # Warehouse (Cómputo) a usar
databasename="CARIBBEAN"          # Base de datos destino
schemaname="CWC_DS"               # Esquema destino

### BLOQUE 4: EJECUCIÓN DEL SQL ###
# Ejecuta snowsql pasando todas las variables de conexión (-a, -u, -r, -w, -d, -s).
# -f ${SQL}: Ejecuta el archivo SQL generado.
# La parte -q "SELECT..." es redundante si se usa -f, pero sirve para validar conexión en el log.
snowsql -a $accountname -u $username -r $rolename -w $warehousename \
-d $databasename -s $schemaname \
-q "SELECT 'Conexión exitosa!' AS RESULTADO;" \
-o header=false -o timing=false -o friendly=false  -f ${SQL}>${LOG}

### BLOQUE 5: VERIFICACIÓN DE ERRORES ###
res=$? # Captura el código de salida

# Busca errores tipo "ORA-" (Legacy de Oracle) en el log si la ejecución fue exitosa.
if [ $res -eq 0 ]; then
        res=`grep "ORA-" ${LOG}|wc -l`
fi

cat ${LOG}
rm ${SQL} # Borra el archivo temporal

if [ $res -ne 0 ]; then
        echo "HUBO ERROR en la carga semanal"
        exit $res
fi

### BLOQUE 6: DEPENDENCIAS (ANÁLISIS DE TENDENCIAS) ###
# Scripts posteriores (actualmente comentados o ejecutados manualmente después).

# 1. Ventana de 12 semanas (Trimestral)
#./gen_abt_jam_12w.sh ${HOY}
res=$?
if [ $res -ne 0 ]; then
        echo "HUBO ERROR en la generacion de abt 12w"
        exit $res
fi

# 2. Ventana de 4 semanas (Mensual Móvil)
# Calcula fecha de inicio restando 28 días a la fecha de corte.
#./gen_abt_jam_4w.sh `date -d "${HOY} - 28 days" '+%Y%m%d'`
res=$?
if [ $res -ne 0 ]; then
        echo "HUBO ERROR en la generacion de abt 4w"
        exit $res
fi