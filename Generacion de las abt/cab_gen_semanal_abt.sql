/* ----------------------------------------------------------------------------------
   NOMBRE: cab_gen_semanal_abt.sql
   OBJETIVO: Generación de Features Semanales para el Caribe (ABT).
   VARIABLE: #DOMINGO# (Fecha de corte de la semana, formato YYYYMMDD).
   ESQUEMA: J_STAGING
   ----------------------------------------------------------------------------------
*/

/* BLOQUE 1: CONFIGURACIÓN DEL CALENDARIO SEMANAL
   Define la ventana de tiempo de la semana actual (Fecha de corte - 6 días).
   Esto asegura que los cálculos siguientes sepan dónde empieza y termina la semana.
*/
DELETE FROM J_STAGING.CAB_ST_ABT_NORECH4W_CAL where fin=DATE('#DOMINGO#','YYYYMMDD');

INSERT INTO J_STAGING.CAB_ST_ABT_NORECH4W_CAL
select 
    DATE('#DOMINGO#','YYYYMMDD')-6,         -- Inicio de la semana (Lunes)
    DATE('#DOMINGO#','YYYYMMDD'),           -- Fin de la semana (Domingo)
    DATE(max(PROCESS_KEY),'YYYYMMDD'),      -- Última fecha procesada real
    'VA'                                    -- Flag de validación
from J_STAGING.CAB_CO_TOKEN_USED
where token='ABTMEN';


/* BLOQUE 2: DEFINICIÓN DE POBLACIÓN (UNIVERSO DE CLIENTES)
   Define qué clientes se considerarán activos para esta semana.
*/
DELETE FROM J_STAGING.CAB_ST_ABT_NORECH4W_SEMANAL_DEF
WHERE DNA_MO_KEY=DATE('#DOMINGO#','YYYYMMDD');

-- Llama al SP que identifica los clientes activos de la semana
--CALL J_STAGING.CAB_ST_ABT_NORECH4W_SEMANAL_DEF(TO_DATE('#DOMINGO#','YYYYMMDD'));
CALL J_STAGING.CAB_ST_ABT_NORECH4W_SEMANAL_DEF('#DOMINGO#');


/* BLOQUE 3: PREPARACIÓN DE DATOS DE RECARGAS
   Ejecuta procedimientos almacenados para limpiar y preparar los datos crudos de recargas.
   El ADD_MONTHS -1 sugiere que se trae historia desde el mes pasado para los cálculos de tendencias.
*/
CALL J_STAGING.CAB_ST_ABT_NORECH4W_SEM_RE_TMP_NORECH(TO_CHAR(ADD_MONTHS(to_date('#DOMINGO#','YYYYMMDD'),-1),'YYYYMM'));
CALL J_STAGING.CAB_ST_ABT_NORECH4W_SEM_RE_TMP_NORECH2('#DOMINGO#');
CALL J_STAGING.CAB_ST_ABT_NORECH4W_SEM_RE_TMP_NORECH3('#DOMINGO#');


/* BLOQUE 4: FEATURE ENGINEERING - RECARGAS (COMPLEJO)
   Aquí ocurre la magia matemática. Se calculan variables de comportamiento.
*/
DELETE FROM J_STAGING.CAB_ST_ABT_NORECH4W_SEM_RE
WHERE domingo=date('#DOMINGO#','YYYYMMDD');

INSERT INTO J_STAGING.CAB_ST_ABT_NORECH4W_SEM_RE
select 
    LNOFBSN_ID,                             -- ID del País/Isla (Diferencia clave con Jamaica)
    b.fin DOMINGO, 
    MSISDN,
    
    -- Métricas de la Semana Actual
    sum((case when domingo=b.fin then cantidad else null end)) cantidad,
    sum((case when domingo=b.fin then monto else null end)) monto,
    avg((case when domingo=b.fin then cantidad else null end)) avg_cantidad,
    avg((case when domingo=b.fin then monto else null end)) avg_monto,
    
    -- Pivot por Día de la Semana (Behavioral Pattern)
    -- Permite saber si el cliente recarga más los fines de semana o días laborales
    sum(case when DIA=1 and  domingo=b.fin then cantidad else 0 end) cantidad_lu,
    sum(case when DIA=2 and  domingo=b.fin then cantidad else 0 end) cantidad_ma,
    sum(case when DIA=3 and  domingo=b.fin then cantidad else 0 end) cantidad_mi,
    sum(case when DIA=4 and  domingo=b.fin then cantidad else 0 end) cantidad_ju,
    sum(case when DIA=5 and  domingo=b.fin then cantidad else 0 end) cantidad_vi,
    sum(case when DIA=6 and  domingo=b.fin then cantidad else 0 end) cantidad_sa,
    sum(case when DIA=7 and  domingo=b.fin then cantidad else 0 end) cantidad_do,
    -- (Mismos pivots pero para Monto $$$) ...
    sum(case when DIA=1 and  domingo=b.fin then monto else 0 end) monto_lu,
    sum(case when DIA=2 and  domingo=b.fin then monto else 0 end) monto_ma,
    sum(case when DIA=3 and  domingo=b.fin then monto else 0 end) monto_mi,
    sum(case when DIA=4 and  domingo=b.fin then monto else 0 end) monto_ju,
    sum(case when DIA=5 and  domingo=b.fin then monto else 0 end) monto_vi,
    sum(case when DIA=6 and  domingo=b.fin then monto else 0 end) monto_sa,
    sum(case when DIA=7 and  domingo=b.fin then monto else 0 end) monto_do,
    
    max(DAY_KEY) ultima, -- Último día que se vio actividad
    
    -- Ventanas Móviles (Rolling Windows) - Tendencias
    -- X1: Corto Plazo (Últimas 3 semanas: días 0 a -21)
    avg((case when domingo between b.fin-21 and  b.fin then cantidad else null end)) PREP_RECH_AVGQ_EVT_X1,
    avg((case when domingo between b.fin-21 and  b.fin then monto else null end)) PREP_RECH_AVGAMT_X1,
    avg((case when domingo between b.fin-21 and  b.fin then monto else null end)/
        (case when domingo between b.fin-21 and  b.fin then cantidad else null end)) PREP_RECH_TKT_X1, -- Ticket Promedio
        
    -- X2: Mediano Plazo (Hace 1 a 2 meses: días -28 a -49)
    avg((case when domingo between  b.fin-49 and b.fin-28 then cantidad else null end)) PREP_RECH_AVGQ_EVT_X2,
    avg((case when domingo between  b.fin-49 and b.fin-28 then monto else null end)) PREP_RECH_AVGAMT_X2,
    avg((case when domingo between  b.fin-49 and b.fin-28 then monto else null end)/
        (case when domingo between  b.fin-49 and b.fin-28 then cantidad else null end)) PREP_RECH_TKT_X2,
        
    -- X3: Largo Plazo (Hace 2 a 3 meses: días -56 a -77)
    avg((case when domingo between b.fin-77 and b.fin-56  then cantidad else null end)) PREP_RECH_AVGQ_EVT_X3,
    avg((case when domingo between b.fin-77 and b.fin-56  then monto else null end)) PREP_RECH_AVGAMT_X3,
    avg((case when domingo between b.fin-77 and b.fin-56  then monto else null end)/
        (case when domingo between b.fin-77 and b.fin-56  then cantidad else null end)) PREP_RECH_TKT_X3

from J_STAGING.CAB_ST_ABT_NORECH4W_SEM_RE_TMP a , J_STAGING.CAB_ST_ABT_NORECH4W_CAL b
where a.domingo between b.fin-77 and  b.fin -- Filtra datos de hasta 77 días atrás
and b.fin=to_date('#DOMINGO#','YYYYMMDD')
group by LNOFBSN_ID,MSISDN,b.fin
order by b.fin;


/* BLOQUE 5: PREPARACIÓN DE DATOS DE PAQUETES (PACKS)
   Prepara las tablas temporales de compra de paquetes.
*/
CALL J_STAGING.CAB_ST_ABT_NORECH4W_SEM_PACK_TMP_NORECH(TO_CHAR(ADD_MONTHS(to_date('#DOMINGO#','YYYYMMDD'),-1),'YYYYMM'));
CALL J_STAGING.CAB_ST_ABT_NORECH4W_SEM_PACK_TMP_NORECH2('#DOMINGO#');
CALL J_STAGING.CAB_ST_ABT_NORECH4W_SEM_PACK_TMP_NORECH3('#DOMINGO#');


/* BLOQUE 6: FEATURE ENGINEERING - PAQUETES
   Transforma las filas (Tipos de paquete) en columnas (Features).
   Esto es una técnica de Pivoteo manual.
*/
DELETE FROM J_STAGING.CAB_ST_ABT_NORECH4W_SEM_PACK
where domingo=date('#DOMINGO#','YYYYMMDD');

insert into J_STAGING.CAB_ST_ABT_NORECH4W_SEM_PACK
select 
    LNOFBSN_ID,
    A.FIN DOMINGO,
    A.MSISDN,
    -- Totales Globales
    sum((case when domingo=a.fin then PACK_cantidad else null end)) PACK_cantidad,
    sum((case when domingo=a.fin then PACK_monto else null end)) PACK_monto,
    avg((case when domingo=a.fin then PACK_cantidad else null end)) PACK_avg_cantidad,
    avg((case when domingo=a.fin then PACK_monto else null end)) PACK_avg_monto,
    
    -- Pivoteo por Tipo de Paquete (DATA)
    sum(case when PACK_TYPE='DATA' and domingo=a.fin then PACK_cantidad else null end) Pack_cantidad_data,
    sum(case when PACK_TYPE='DATA' and domingo=a.fin then PACK_monto else null end) Pack_monto_data,
    
    -- Pivoteo por Tipo de Paquete (VOICE)
    sum(case when PACK_TYPE='VOICE' and domingo=a.fin then PACK_cantidad else null end) Pack_cantidad_voice,
    sum(case when PACK_TYPE='VOICE' and domingo=a.fin then PACK_monto else null end) Pack_monto_voice,
    
    -- Pivoteo por Tipo de Paquete (Híbridos)
    sum(case when PACK_TYPE='VOICEDATA' and domingo=a.fin then PACK_cantidad else null end) Pack_cantidad_voicedata,
    sum(case when PACK_TYPE='VOICEDATA' and domingo=a.fin then PACK_monto else null end) Pack_monto_voicedata,
    sum(case when PACK_TYPE='VOICESMSDATA' and domingo=a.fin then PACK_cantidad else null end) Pack_cantidad_voicesmsdata,
    sum(case when PACK_TYPE='VOICESMSDATA' and domingo=a.fin then PACK_monto else null end) Pack_monto_voicesmsdata,
    
    max(DAY_KEY) PACK_ultima
FROM (
    -- Subquery: Agrupa primero por tipo de paquete para simplificar el pivoteo externo
    select LNOFBSN_ID,msisdn,domingo,max(day_key) day_key,b.FIN,pack_type,sum(pack_monto) pack_monto, sum(pack_cantidad) pack_cantidad
    from J_STAGING.CAB_ST_ABT_NORECH4W_SEM_PACK_TMP_NORECH a inner join J_STAGING.CAB_ST_ABT_NORECH4W_CAL b
    on a.domingo=b.fin
    where b.fin=to_date('#DOMINGO#','YYYYMMDD')
    group by LNOFBSN_ID,msisdn,domingo,pack_type, b.FIN
) A
GROUP BY LNOFBSN_ID,A.MSISDN,A.FIN
ORDER BY A.FIN;


/* BLOQUE 7: TOKEN DE FINALIZACIÓN
   Marca el proceso como terminado exitosamente insertando un token en la tabla de control.
   Esto permite que los scripts 'downstream' (12w, 4w) sepan que pueden iniciar.
*/
insert into J_STAGING.CAB_CO_TOKEN_USED values ('ABTSEM','#DOMINGO#');