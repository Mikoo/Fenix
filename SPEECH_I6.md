# Speech — I6: Pruebas y Diagnóstico (Proyecto Fénix)

**Duración estimada:** 6-7 minutos
**Rol:** Integrante 6 — Pruebas y Diagnóstico
**Reglamento:** Sección 2.2 — identificar rol, mostrar módulo, explicar decisión, demostrar caso correcto y caso de fallo.

---

## [0:00 – 0:30] Identificación del rol

> "Hola, soy Sebastián Paz Villarreal, integrante 6 del equipo. Mi rol es **Pruebas y Diagnóstico**. No implemento la lógica de negocio — eso lo hacen mis compañeros de I1 a I5. Lo que hago yo es **fabricar fallos controlados** para verificar que el sistema los detecta y repara, y **diseñar las pruebas** que demuestran que cada pieza funciona. Soy, en criollo, el 'rompe-cosas' del equipo: si algo no se rompe bien, no se puede probar bien."

## [0:30 – 1:30] inject_fault — qué hace y por qué

> "Mi función principal es `inject_fault`, en `services/demo.py`. Recibe un alias: `node-01` o `bucket-01`. **Nunca recibe un InstanceId o un ARN libre** — eso es una decisión de seguridad. Si aceptara un ID arbitrario, un evento malicioso podría reparar un recurso que no es del grupo. El inventario (`data/inventory.json`) es la única fuente de verdad de qué recursos le pertenecen a este grupo.

> Para `node-01`: llamo a `stop_instances` y luego hago polling acotado — 10 intentos de 1 segundo — hasta que el estado sea `stopped`. No uso `terminate` porque el caso de negocio es 'nodo detenido → arrancarlo'; si lo destruyera, no habría nada que reparar.

> Para `bucket-01`: quito la etiqueta `Owner` conservando el resto de las tags. Simulo la pérdida parcial para que la reparación de I3 mezcle y verifique.

> Y una cosa importante: `inject_fault` **no envía el evento automáticamente**. Solo fabrica el problema. El operador envía el evento con `fenix send` cuando quiere disparar la reparación. Así puedo preparar el fallo, mostrarlo en pantalla, y luego enviar el evento en vivo para la demo."

## [1:30 – 2:30] El stub de notify

> "El segundo componente es el stub de `notify`, en `tests/notify_stub.py`. El problema: para probar el caso F14 — 'mock de aviso 429/timeout y luego éxito' — necesito simular que Telegram o Slack devuelve un 429 o un timeout, **sin spamear un canal real ni depender de internet**.

> El stub es una clase `NotifyStub` que se comporta como `notify` pero en memoria. Tiene tres modos: `ok` (éxito), `rate_limit` (lanza `RetryableError` N veces y luego éxito), y `permanent` (lanza `PermanentError`). Además registra todas las llamadas — cuántas veces se invocó y con qué texto — para que el test pueda verificar el orden.

> Esto es clave para la idempotencia: si `notify` falla con 429, el worker **no repite la reparación** (porque `repair_done == True` en el ledger), solo reintenta el aviso. El stub me permite demostrar eso sin tocar un proveedor real."

## [2:30 – 3:30] Tests y matriz

> "Implementé 8 tests unitarios en `test_fenix.py` y 3 de escenarios en `test_scenarios.py`. Los unitarios no necesitan LocalStack — usan `tmp_path` y `monkeypatch` para crear un inventario falso, y `patch` de `client` para simular EC2 y S3.

> Por ejemplo, `test_inject_fault_node_stops` verifica que se llame a `stop_instances` con el InstanceId del inventario, y que luego se consulte `describe_instances` para confirmar `stopped`. Sin tocar ningún recurso real.

> También hay un test estático — `test_no_test_deletes_other_group_resources` — que verifica con `inspect.getsource` que `inject_fault` solo use el alias del inventario. Esto es para cumplir la regla del doc: 'revisar que ningún test borre recursos de otro grupo'."

## [3:30 – 4:30] Caso correcto y caso de fallo

> **Caso correcto:** inventario válido con `node-01`. Ejecuto `inject_fault("node-01")`. El resultado: la instancia pasa a `stopped`, verificado con `describe_instances`. El test pasa.

> **Caso de fallo:** inventario vacío o alias no presente. `inject_fault("bucket-01")` con un inventario que solo tiene `node-01`. El resultado: `ValidationError` con mensaje 'Alias no presente en el inventario'. No se toca ningún recurso. El test pasa.

> **Caso de fallo del stub:** `NotifyStub` en modo `rate_limit` con count=1. La primera llamada lanza `RetryableError` (429). La segunda llamada ya no falla. El test verifica que se registraron 2 llamadas. Esto demuestra que el sistema puede reintentar el aviso sin repetir la reparación.

## [4:30 – 5:30] Decisión de diseño

> "La decisión más importante de mi rol fue **validar el alias explícitamente** además de verificar que esté en el inventario. Podría haberme limitado a 'si está en el inventario, aceptalo'. Pero eso es una sola capa de seguridad. Agregué una validación explícita: `if alias not in ("node-01", "bucket-01")`. Es defense in depth: la CLI ya limita con `choices`, y la función refuerza esa restricción a nivel de código. Si mañana alguien modifica el inventario y agrega un alias extraño, `inject_fault` lo rechaza igual."

## [5:30 – 6:00] Cierre y defensa

> "Para cerrar: la diferencia entre un **log escrito por el programa** y una **prueba que consulta el estado con Boto3** es fundamental. El logger JSON (`logging_json.py`) registra lo que el programa *cree* que hizo — por ejemplo, 'stop_instances llamado'. Pero eso no prueba que el recurso realmente cambió de estado. La prueba de I6 consulta con `describe_instances` o `get_bucket_tagging` para verificar el estado real. Un log dice 'intenté'; Boto3 dice 'verifiqué'. Esa es la diferencia entre una afirmación y una evidencia.

> Gracias. Quedo a disposición para preguntas."

---

## Notas para la grabación

- **Duración estimada:** ~6 minutos (dentro del rango 5-8 del reglamento).
- **Pantalla:** mostrar `demo.py` (inject_fault), `notify_stub.py`, y la ejecución de `pytest tests/test_fenix.py -k "inject_fault or notify_stub"`.
- **Evidencia:** mostrar el output de pytest con los tests pasando.
- **Defensa en vivo:** si te piden mostrar un caso de fallo, ejecutá `inject_fault("bucket-01")` con un inventario que solo tiene `node-01` y mostrá el `ValidationError`.