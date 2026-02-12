#!/bin/bash

### BLOQUE 1: CÁLCULO DE LA FECHA DE CORTE (DOMINGO) ###
# 'last Sunday': Busca el domingo inmediato anterior a la fecha actual.
# Formato: YYYYMMDD (Ej: 20260208).
# Esta es la variable más importante, define el cierre de la semana.
HOY=`date  -d "last Sunday" '+%Y%m%d'`

### BLOQUE 2: PREPARACIÓN DE ARCHIVOS ###
# SQLORI: La plantilla SQL base. Busca este archivo, ahí está la lógica de negocio.
SQLORI=cab_gen_semanal_abt.sql

# SQL: Archivo temporal donde se escribirá el código final.
SQL=/tmp/cab_gen_semanal_abt.${HOY}.sql
echo >> ${SQL} # Crea archivo vacío

# LOG: Archivo para capturar errores o mensajes de éxito.
LOG=/tmp/cab_gen_semanal_abt.${HOY}.log
echo >> ${LOG}

### BLOQUE 3: INYECCIÓN DE VARIABLES (TEMPLATING) ###
# Usa 'sed' para buscar la palabra clave '#DOMINGO#' en la plantilla
# y reemplazarla por la fecha calculada (Ej: 20260208).
cat ${SQLORI}|sed -e "s/#DOMINGO#/${HOY}/g" >${SQL}

# Muestra el SQL generado en pantalla para debug.
cat ${SQL}

### BLOQUE 4: EJECUCIÓN EN SNOWFLAKE ###
# Ejecuta el script. Nota: Aquí faltan las banderas de usuario/password (-u, -p) 
# que vimos en scripts anteriores. Asumo que están configuradas en el entorno o en config.
snowsql -o header=false -o timing=false -o friendly=false  -f ${SQL}>${LOG}

### BLOQUE 5: MANEJO DE ERRORES (LEGACY) ###
res=$? # Captura código de salida

# Validación heredada de Oracle: Busca "ORA-" en el log aunque estemos en Snowflake.
if [ $res -eq 0 ]; then
        res=`grep "ORA-" ${LOG}|wc -l`
fi

cat ${LOG}
rm ${SQL} # Limpieza del archivo temporal

# Si falló, detiene todo el proceso.
if [ $res -ne 0 ]; then
        echo "HUBO ERROR en la carga semanal"
        exit $res
fi

### BLOQUE 6: DEPENDENCIAS DESCENDENTES (DOWNSTREAM) ###
# Estos scripts están comentados (#), pero indican el flujo lógico del negocio.
# Una vez generada la ABT Semanal, se deberían calcular las ventanas móviles:

# 1. Ventana de 12 Semanas (Tendencia trimestral)
#./gen_abt_cwc_12w.sh ${HOY}
res=$?
if [ $res -ne 0 ]; then
        echo "HUBO ERROR en la generacion de abt 12w"
        exit $res
fi

# 2. Ventana de 4 Semanas (Tendencia mensual móvil)
# Nota la matemática de fechas: Resta 28 días a la fecha de corte para el cálculo.
#./gen_abt_cwc_4w.sh `date -d "${HOY} - 28 days" '+%Y%m%d'`
res=$?
if [ $res -ne 0 ]; then
        echo "HUBO ERROR en la generacion de abt 4w"
        exit $res
fi