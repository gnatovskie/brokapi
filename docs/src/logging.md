# Логи

С версии 0.1.0-beta.1 лог библиотеки включается одним вызовом. По логу видно, что делали коннекторы: подключение,
подписки, заявки, обрывы. Сама библиотека лог не включает: без вызова строк нет и затрат на них почти нет.

## Включить

Rust, флаг `logging` в `Cargo.toml` (`features = ["bcs", "logging"]`):

```rust,ignore
fn main() -> Result<(), Box<dyn std::error::Error>> {
    brokapi::logging::init(None)?; // уровень из BROKAPI_LOG, иначе info
    // ... дальше коннекторы
    Ok(())
}
```

Python:

```python
import brokapi

brokapi.enable_logging()          # уровень из BROKAPI_LOG, иначе info
brokapi.enable_logging("debug")   # или явно
```

Служба `brokapi` пишет лог всегда, в файлы каталога состояния (`brokapi logs`), и читает ту же переменную.

Строки идут в stderr. Включить можно один раз на процесс: второй вызов в Rust вернет `Error::AlreadySet`, в Python
вернет `False`.

Вызывайте до старта коннекторов. Коннектор, запущенный раньше, пишет строки без своего контекста: без брокера и счета.

## Уровень

Уровень выбирается так: аргумент вызова, если он есть, иначе переменная `BROKAPI_LOG`, иначе `info`.

| Значение | Что в логе |
|---|---|
| `warn` | только сбои, которые библиотека пережила сама: обрывы, переподключения, отказы токена |
| `info` | плюс жизнь коннекторов и итог каждой торговой команды. Подходит для работы бота |
| `debug` | плюс каждый запрос к брокеру. Для разбора сбоя |
| `trace` | плюс кадры потоков брокера после чистки, у Bybit и тела ответов HTTP. Много строк, только на время разбора |

Уровень `info`, `debug` или `trace` без модуля относится к brokapi. Библиотеки HTTP и gRPC под коннекторами
остаются на `warn`, иначе на `debug` между строками коннекторов шли бы строки каждого соединения. Включить их
можно явно: `BROKAPI_LOG=debug,hyper=debug`.

Можно задать уровень по модулю: `BROKAPI_LOG=info,brokapi_bcs=debug` оставит `info` для остальных модулей brokapi и
включит `debug` только для БКС. Модуль, который написал строку, виден в каждой строке, например у службы:

```text
2026-11-20T10:00:00.123456Z  INFO brokapi_gateway::service::supervisor: taken over a running gateway gateway=bcs-main pid=4182
```

Неверный аргумент вызова - ошибка (`BadDirective` в Rust, `ValueError` в Python). Неверная переменная не останавливает
бота: лог включается на `info`, первой строкой идет предупреждение `log variable ignored`.

## Заявки в логе

Каждая смена состояния заявки - одна строка с ее `cid`:

| Строка | Уровень |
|---|---|
| `order acked` (с `broker_order_id`), `order filled`, `order cancelled`, `order rejected` (с `reason`) | `info` |
| `order partially filled` (с `filled` и `left`) | `debug` |
| `order outcome unknown`, `no answer to the command in time` | `warn` |

Строка `warn` об исходе заявки значит: заявка у брокера может быть. Команду не повторяйте, вызовите `reconcile`
(раздел [Если что-то пошло не так](rust/recovery.md)).

Перед текстом строки стоит ее контекст: какой коннектор (брокер, роль, счет маской) и какая команда (`op`, `cid`,
инструмент). Так строка запроса на `debug` показывает, к какой заявке она относится. Команда заканчивается строкой
`command done` на `info`. Если команда вернула ошибку, строка идет на `debug`, а `Unknown` пишется на `warn`.

```text
2026-11-20T10:00:01.204518Z  INFO connector{broker="tbank" role="ud" account="****3456"}:op{op="place" cid=5f0c7a1e-2b0d-4c1e-9a43-3f9d2c1b7e10 instrument=SBER@TQBR}: brokapi_runtime::orders: order acked cid=5f0c7a1e-2b0d-4c1e-9a43-3f9d2c1b7e10 broker_order_id=48213377
```

## Что в строке запроса

На `debug` каждый запрос к брокеру - строка `request done` с полями `method` (`POST /v5/order/create`,
`OrdersService/PostOrder`), `status` (код HTTP, у gRPC еще `grpc_status`) и `latency_ms`. Запрос, который не дошел, -
строка `request failed` с ошибкой.

## Что не попадает в лог

- Токены, ключи API и подписи - ни на каком уровне.
- Номер счета - только маской, последние 4 символа: `****3456`. В пути запроса тоже.
- На `info` и `debug` тела запросов и кадры потоков не пишутся. На `trace` чистка ищет поля по именам из документации и записанных ответов (вживую `trace`-журналы не проверялись): перед передачей журнала с живого счета просмотрите его глазами. Поля с токенами и личными данными заменяются на `<redacted>`.

## Свой подписчик

Если у программы уже есть свой вывод `tracing`, возьмите только правила уровня, `brokapi::logging::filter`:

```rust,ignore
use tracing_subscriber::prelude::*;

let filter = brokapi::logging::filter(None)?;
tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer().with_filter(filter))
    .init();
```

Свой подписчик тоже ставьте до старта коннекторов.
