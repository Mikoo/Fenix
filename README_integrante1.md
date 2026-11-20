# Proyecto Fénix - Integrante 1

## Responsabilidad

Validación de entradas, contratos de eventos
y revisión de excepciones del sistema.

## Funcionalidades implementadas

### I1-A - Identificadores
- Validación de identificadores de 3 a 32 caracteres.
- Admite letras minúsculas, números y guiones.
- Rechazo de rutas y caracteres no autorizados.

### I1-B - TTL
- Conversión de texto a número entero.
- Validación del rango de 60 a 600 segundos.
- Manejo de excepciones mediante ValidationError.

### I1-C - Fechas UTC
- Interpretación de fechas ISO 8601.
- Validación de zona horaria UTC explícita.
- Rechazo de fechas sin zona o con offset no nulo.

### I1-D - Validación de eventos
- Validación de esquema cerrado.
- Control de versión del esquema.
- Validación de identificadores y fechas.
- Control de recursos autorizados mediante inventario.
- Compatibilidad entre evento y tipo de recurso.
- Validación de fallos simulados entre 0 y 5.
- Rechazo de booleanos como valores enteros.
- Registro de eventos mediante event_id.
- Detección de conflictos por contenido diferente.
- Admisión de reintentos con contenido idéntico.
- Compatibilidad con persistencia JSON.
- Registro de intentos, reparación y notificación
  preparado para la integración con el worker.

## Excepciones revisadas

- ProjectError
- ValidationError
- RetryableError
- PermanentError

## Contrato de inventario

- Instancias EC2: type = instance.
- Buckets S3: type = bucket.
- Identificadores autorizados mediante aliases.
- Identificadores físicos obtenidos del inventario.

## Pruebas realizadas

- 3 pruebas originales de contratos aprobadas.
- 21 pruebas adicionales de eventos aprobadas.
- 6 pruebas de conflictos y persistencia aprobadas.
- 2 ejemplos JSON originales validados.
- Verificación de sintaxis exitosa.

## Ejecución de pruebas

    python -m pytest tests/test_contracts.py tests/test_events.py tests/test_event_conflicts.py -v

## Resultados

30 pruebas aprobadas, sin fallos.

## Evidencias

Archivo evidencia_integrante1.txt.

## Integración pendiente

- Generar el inventario real respetando el contrato definido.
- Incorporar register_event() en el código del integrante 5.
- Persistir el registro desde el worker.
- Verificar la integración con el receptor y el worker.