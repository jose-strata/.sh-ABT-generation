/* ----------------------------------------------------------------------------------
   NOMBRE: jam_gen_semanal_abt_v2.sql
   OBJETIVO: Generación de Features Semanales para Jamaica (ABT).
   VARIABLE: #DOMINGO# (Fecha de corte de la semana, formato YYYYMMDD).
   ESQUEMA: CWC_DS
   ----------------------------------------------------------------------------------
*/

/* BLOQUE 1: GESTIÓN DEL CALENDARIO SEMANAL
   Establece la ventana de tiempo. Se asegura de limpiar cualquier registro previo
   para esa fecha de corte antes de insertar el nuevo periodo.
*/
DELETE FROM CWC_DS.ST_ABT_NORECH4W_CAL where fin=DATE('#DOMINGO#','YYYYMMDD');

INSERT INTO CWC_DS.ST_ABT_NORECH4W_CAL
select 
    DATE('#DOMINGO#','YYYYMMDD')-6,         -- Lunes (Inicio semana)
    DATE('#DOMINGO#','YYYYMMDD'),           -- Domingo (Fin semana/Corte)
    DATE(max(PROCESS_KEY),'YYYYMMDD'),      -- Última fecha de proceso válida
    'VA'                                    -- Flag de validación
from CWC_DS.CO_TOKEN_USED
where token='ABTMEN'; -- Busca el token mensual para asegurar consistencia


/* BLOQUE 2: POBLACIÓN ACTIVA (CLIENTES)
   Define qué clientes se van a procesar.
*/
DELETE FROM CWC_DS.st_abt_norech4w_semanal_def
WHERE DNA_MO_KEY=DATE('#DOMINGO#','YYYYMMDD');

-- Llama al procedimiento que identifica la base de clientes activos
-- Nótese que pasa el mes (YYYYMM) y el día completo (#DOMINGO#)
CALL CWC_DS.ST_ABT_NORECH4W_SEMANAL_DEF(TO_CHAR(TO_DATE('#DOMINGO#','YYYYMMDD'),'YYYYMM'),'#DOMINGO#');


/* BLOQUE 3: PRE-PROCESAMIENTO DE RECARGAS
   Prepara las tablas temporales con los datos crudos de recargas.
   Trae datos desde el mes anterior (-1 month) para poder calcular tendencias.
*/
CALL CWC_DS.ST_ABT_NORECH4W_SEM_RE_TMP_NORECH(TO_CHAR(ADD_MONTHS(to_date('#DOMINGO#','YYYYMMDD'),-1),'YYYYMM'));
CALL CWC_DS.ST_ABT_NORECH4W_SEM_RE_TMP_NORECH2('#DOMINGO#');
CALL CWC_DS.ST_ABT_NORECH4W_SEM_RE_TMP_NORECH3('#DOMINGO#');


/* BLOQUE 4: FEATURE ENGINEERING - RECARGAS (PIVOT & WINDOWS)
   Calcula KPIs de comportamiento de recarga.
*/
delete from CWC_DS.st_abt_norech4w_sem_re
where domingo=date('#DOMINGO#','YYYYMMDD');

insert into CWC_DS.st_abt_norech4w_sem_re
select 
    b.fin DOMINGO, 
    MSISDN,
    
    -- Métricas de la Semana Actual
    sum((case when domingo=b.fin then cantidad else null end)) cantidad,
    sum((case when domingo=b.fin then monto else null end)) monto,
    avg((case when domingo=b.fin then cantidad else null end)) avg_cantidad,
    avg((case when domingo=b.fin then monto else null end)) avg_monto,
    
    -- Desglose por Día de la Semana (Lun-Dom) para Cantidad y Monto
    -- Ayuda a entender si el cliente es "pagador de fin de semana"
    sum(case when DIA=1 and  domingo=b.fin then cantidad else 0 end) cantidad_lu,
    sum(case when DIA=2 and  domingo=b.fin then cantidad else 0 end) cantidad_ma,
    sum(case when DIA=3 and  domingo=b.fin then cantidad else 0 end) cantidad_mi,
    sum(case when DIA=4 and  domingo=b.fin then cantidad else 0 end) cantidad_ju,
    sum(case when DIA=5 and  domingo=b.fin then cantidad else 0 end) cantidad_vi,
    sum(case when DIA=6 and  domingo=b.fin then cantidad else 0 end) cantidad_sa,
    sum(case when DIA=7 and  domingo=b.fin then cantidad else 0 end) cantidad_do,
    
    sum(case when DIA=1 and  domingo=b.fin then monto else 0 end) monto_lu,
    sum(case when DIA=2 and  domingo=b.fin then monto else 0 end) monto_ma,
    sum(case when DIA=3 and  domingo=b.fin then monto else 0 end) monto_mi,
    sum(case when DIA=4 and  domingo=b.fin then monto else 0 end) monto_ju,
    sum(case when DIA=5 and  domingo=b.fin then monto else 0 end) monto_vi,
    sum(case when DIA=6 and  domingo=b.fin then monto else 0 end) monto_sa,
    sum(case when DIA=7 and  domingo=b.fin then monto else 0 end) monto_do,
    
    max(DAY_KEY) ultima,
    
    -- Ventanas Móviles (Rolling Windows) para detectar cambios de comportamiento
    -- X1: Corto Plazo (Últimas 3 semanas)
    avg((case when domingo between b.fin-21 and  b.fin then cantidad else null end)) PREP_RECH_AVGQ_EVT_X1,
    avg((case when domingo between b.fin-21 and  b.fin then monto else null end)) PREP_RECH_AVGAMT_X1,
    avg((case when domingo between b.fin-21 and  b.fin then monto else null end)/
        (case when domingo between b.fin-21 and  b.fin then cantidad else null end)) PREP_RECH_TKT_X1,
        
    -- X2: Mediano Plazo (Hace 1-2 meses)
    avg((case when domingo between  b.fin-49 and b.fin-28 then cantidad else null end)) PREP_RECH_AVGQ_EVT_X2,
    avg((case when domingo between  b.fin-49 and b.fin-28 then monto else null end)) PREP_RECH_AVGAMT_X2,
    avg((case when domingo between  b.fin-49 and b.fin-28 then monto else null end)/
        (case when domingo between  b.fin-49 and b.fin-28 then cantidad else null end)) PREP_RECH_TKT_X2,
        
    -- X3: Largo Plazo (Hace 2-3 meses)
    avg((case when domingo between b.fin-77 and b.fin-56  then cantidad else null end)) PREP_RECH_AVGQ_EVT_X3,
    avg((case when domingo between b.fin-77 and b.fin-56  then monto else null end)) PREP_RECH_AVGAMT_X3,
    avg((case when domingo between b.fin-77 and b.fin-56  then monto else null end)/
        (case when domingo between b.fin-77 and b.fin-56  then cantidad else null end)) PREP_RECH_TKT_X3

from CWC_DS.st_abt_norech4w_sem_re_tmp a , CWC_DS.ST_ABT_NORECH4W_CAL b
where a.domingo between b.fin-77 and  b.fin
and b.fin=to_date('#DOMINGO#','YYYYMMDD')
group by MSISDN,b.fin
order by b.fin;


/* BLOQUE 5: PRE-PROCESAMIENTO DE PAQUETES
   Llama a los procedimientos para limpiar/preparar datos de compra de paquetes.
*/
CALL CWC_DS.ST_ABT_NORECH4W_SEM_PACK_TMP_NORECH(TO_CHAR(ADD_MONTHS(to_date('#DOMINGO#','YYYYMMDD'),-1),'YYYYMM'));
CALL CWC_DS.ST_ABT_NORECH4W_SEM_PACK_TMP_NORECH2('#DOMINGO#');
CALL CWC_DS.ST_ABT_NORECH4W_SEM_PACK_TMP_NORECH3('#DOMINGO#');


/* BLOQUE 6: FEATURE ENGINEERING - PAQUETES
   Pivotea los tipos de paquetes en columnas individuales.
*/
delete from CWC_DS.st_abt_norech4w_sem_pack
where domingo=date('#DOMINGO#','YYYYMMDD');

insert into CWC_DS.st_abt_norech4w_sem_pack
select 
    A.FIN DOMINGO,
    A.MSISDN,
    -- Totales Globales
    sum((case when domingo=a.fin then PACK_cantidad else null end)) PACK_cantidad,
    sum((case when domingo=a.fin then PACK_monto else null end)) PACK_monto,
    avg((case when domingo=a.fin then PACK_cantidad else null end)) PACK_avg_cantidad,
    avg((case when domingo=a.fin then PACK_monto else null end)) PACK_avg_monto,
    
    -- Desglose por Tipo (DATA, VOICE, Híbridos)
    sum(case when PACK_TYPE='DATA' and domingo=a.fin then PACK_cantidad else null end) Pack_cantidad_data,
    sum(case when PACK_TYPE='DATA' and domingo=a.fin then PACK_monto else null end) Pack_monto_data,
    sum(case when PACK_TYPE='VOICE' and domingo=a.fin then PACK_cantidad else null end) Pack_cantidad_voice,
    sum(case when PACK_TYPE='VOICE' and domingo=a.fin then PACK_monto else null end) Pack_monto_voice,
    sum(case when PACK_TYPE='VOICEDATA' and domingo=a.fin then PACK_cantidad else null end) Pack_cantidad_voicedata,
    sum(case when PACK_TYPE='VOICEDATA' and domingo=a.fin then PACK_monto else null end) Pack_monto_voicedata,
    sum(case when PACK_TYPE='VOICESMSDATA' and domingo=a.fin then PACK_cantidad else null end) Pack_cantidad_voicesmsdata,
    sum(case when PACK_TYPE='VOICESMSDATA' and domingo=a.fin then PACK_monto else null end) Pack_monto_voicesmsdata,
    
    max(DAY_KEY) PACK_ultima
FROM (
    -- Subquery de agregación preliminar
    select msisdn,domingo,max(day_key) day_key,b.FIN,pack_type,sum(pack_monto) pack_monto, sum(pack_cantidad) pack_cantidad
    from CWC_DS.ST_ABT_NORECH4W_SEM_PACK_TMP_NORECH a inner join CWC_DS.ST_ABT_NORECH4W_CAL b
    on a.domingo=b.fin
    where b.fin=to_date('#DOMINGO#','YYYYMMDD')
    group by msisdn,domingo,pack_type, b.FIN
) A
GROUP BY A.MSISDN,A.FIN
ORDER BY A.FIN;

/* BLOQUE 7: FINALIZACIÓN
   Inserta token de control indicando éxito.
*/
insert into CWC_DS.co_token_used values ('ABTSEM','#DOMINGO#');