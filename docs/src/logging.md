# Логи

С версии 0.1.0-alpha.11 лог библиотеки включается одним вызовом. По логу видно, что делали коннекторы: подключение,
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

## Уровень

Уровень выбирается так: аргумент вызова, если он есть, иначе переменная `BROKAPI_LOG`, иначе `info`.

| Значение | Что в логе |
|---|---|
| `warn` | только сбои, которые библиотека пережила сама: обрывы, переподключения, отказы токена |
| `info` | плюс жизнь коннекторов и итог каждой торговой команды. Подходит для работы бота |
| `debug` | плюс каждый запрос к брокеру. Для разбора сбоя |
| `trace` | плюс тела запросов и ответов. Много строк, только на время разбора |

Можно задать уровень по модулю: `BROKAPI_LOG=info,brokapi_bcs=debug` оставит `info` для всех и включит `debug` только
для БКС. Модуль, который написал строку, виден в каждой строке, например у службы:

```text
2026-11-20T10:00:00.123456Z  INFO brokapi_gateway::service::supervisor: taken over a running gateway gateway=bcs-main pid=4182
```

Неверный аргумент вызова - ошибка (`BadDirective` в Rust, `ValueError` в Python). Неверная переменная не останавливает
бота: лог включается на `info`, первой строкой идет предупреждение `log variable ignored`.

## Свой подписчик

Если у программы уже есть свой вывод `tracing`, возьмите только правила уровня, `brokapi::logging::filter`:

```rust,ignore
use tracing_subscriber::prelude::*;

let filter = brokapi::logging::filter(None)?;
tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer().with_filter(filter))
    .init();
```
