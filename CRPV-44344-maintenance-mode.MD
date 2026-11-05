# CRPV-44344 Maintenance mode

* Задача
* Условия и ограничения
* Описание реализации 
* Термины и определения
* Ссылки

## Задача

### Режим обслуживания КЛАСТЕРА

#### Проблема 1

Необходима индикация для пользователя о том, что на кластере проводятся работы.
При этом пользователь будет знать, что некоторые операции могут не работать.

#### Проблема 2

Администраторы обслуживают множество "одинаковых" кластеров.
Зачастую производимые работы носят
* Ручной.
* Повторяющийся (нужно обслужить ВСЕ кластера) характер.

При выполнении подобного рода работ возможны механические ошибки оператора "Запустил команду не на том кластере".
Что может привести к ошибкам и некорректной работе.

Для избежания подобных ситуаций необходимо выделить режим обслуживания кластера.
Администратор переводит кластер в режим обслуживания перед началом работ.
Команды обслуживания выполняются только в режиме обслуживания.
Таким образом, при ошибке вида "Запустил команду не на том кластере" администратор получит понятную ошибку и не сможет загрузить кластер ненужной задачей.

## Условия и ограничения

* Версия 19.0
* Настроек плагин безопасности.

### RU 

В процессе обновления переход в режим maintenance отключен.
Работает "по-старому" - все команды доступны для выполнения в любом режиме.

## Описание реализации 

### Описание процесса выполнения работ после реализации режима обслуживания

Процесс проведения работ после реализации выглядит следующим образом:
1. В обычном режиме работы кластера попытка выполнения команды обслуживания заканчивается ошибкой "Команда доступна только в maintenance mode".
2. Кластер можно перевести в maintenance mode с помощью команды control.sh. После перехода:
    * Уже работающие подключения НЕ прерываются.
    * Команды обслуживания выполняются.
    * В подсистему Аудита отправляется сообщение о переходе кластера в режим обслуживания.
3. После перевода клиенты переключаются на резервный кластер (процесс переключения реализуется внешними средствами). 
    * Возможность принудительно отключить клиентские соединения реализована `KILL` командами.
4. Выполняются работы.
5. По окончании работ кластер переводится в обычный режим.
   * Новые клентские подключения разрешены.
   * Команды обслуживания НЕ выполняются.

### Описание реализации

1. Вводится новый режим кластера. Список режимов кластера - **`MAINTENANCE`**, `INACTIVE`, `ACTIVE`, `ACTIVE_READ_ONLY`.
   * В `MAINTENANCE` режим кластер переводится ТОЛЬКО из `ACTIVE` режима.
2. Для каждой команды вводится признак `maintenanceOnly`. Если признак равен `true` то команда доступна для выполнения только в `MAINTENANCE` режиме.
3. (?) Разделение на режимы обычный/обслуживание включается флагов в конфигуарции `IgniteConfiguration#maintenanceModeEnabled`. Дефолт - `false`. Для сохранения обратной совместимости.

### Команды обслуживания 

  1. (?) `--baseline --auto-adjust` - Set baseline autoadjustment settings.
  2. (?) `--baseline add` - Add nodes into baseline topology.
  3. (?) `--baseline remove` - Remove nodes from baseline topology.
  4. (?) `--baseline set` - Set baseline topology.
  5. (?) `--cache idle-verify` - Verify counters and hash sums of primary and backup partitions.
  6. `--cache idle-verify --dump` - Calculate partition hash and print into standard output.
  7. `--cache destroy` - Permanently destroy specified caches.
  8. (?) `--cache clear` - Clear specified caches.
  9. `--cache validate_indexes` - Validates indexes.
  10. `--cache reset_lost_partitions` - Reset the state of lost partitions for the specified cache groups.
  11. `--cache find_garbage` - Find and optionally delete garbage from shared cache groups which could be left after cache destroy.
  12. `--cache indexes_force_rebuild` - Triggers rebuild of all indexes for specified caches or cache groups.
  13. `--cache schedule_indexes_rebuild` - Schedules rebuild of the indexes for specified caches via the Maintenance Mode.
  14. (?) `--snapshot restore` - Restore snapshot.
  15. `--meta remove` - Remove the metadata of the specified type.
  16. `--meta update` - Update cluster metadata from specified file.
  17. `--warm-up --stop` - Stop warm-up.
  18. `--persistence clean corrupted` - Clean directories of caches with corrupted data files. 
  19. `--persistence clean all` - Clean directories of all caches.
  20. `--persistence clean caches` - Clean directories of only given caches.
  21. `--persistence backup corrupted` - Backup data files of corrupted caches only. 
  22. `--persistence backup all` - Backup data files of all caches.
  23. `--persistence backup caches` - Backup data files of only given caches.
  24. `--defragmentation schedule` - Schedule PDS defragmentation.
  25. `--cdc delete_lost_segment_links` - Delete lost segment CDC links.
  26. `--cdc resend` - Forcefully resend all cache data to CDC.
  27. (?) `--consistency repair` - Check/Repair cache consistency using Read Repair approach.
  28. `--consistency finalize` - Finalize partitions update counters. 
  29. (?) `--event enable` - Enable events on all server nodes.
  30. (?) `--event disable` - Disable events on all server nodes.
  31. Можно запретить новым клиентским подключениям (thin, client node, JDBC, ODBC).
        * Например, cdc-client - можно подключаться, user - нельзя подключаться.

## Термины и определения

* **Maintenance mode** - режим кластера в котором:
  * Запрещены новые клиентские подключения.
  * Разрешено выполнление команд обслуживания кластером (точный список в "Описание реализации").
    * Обратно - когда maintenance mode НЕактивен (обычная работа кластера) - выполнение команд обслуживания запрещено.

## Ссылки

  1. [CRPV-53506 Реализовать ограниченный режим подключения к кластеру](https://sberworks.ru/jira/browse/CRPV-53506)
  2. [CRPV-75091 Частичный maintenance mode](https://sberworks.ru/jira/browse/CRPV-75091)
  3. [CRPV-44344 Поддержать maintenance mode](https://sberworks.ru/jira/browse/CRPV-44344)
  4. [CRPV-161490 Maintenance Mode: обслуживание DataGrid с ограничением клиентской нагрузки](https://sberworks.ru/jira/browse/CRPV-161490)
  5. [IGN-2453](https://portal.works.prod.sbt/swtr/units/current/unit/IGN-2453)

## Оценка сложности

Задача перевода кластера в особый новый режим СЛОЖНАЯ.
Оценка в трудоднях - 100 ч.д

## Дополнительные задачи

1. Запрет подключений в разрезе "роли клиента" - cdc-client, user, admin.
2. Дополнительные команды для maintenance mode - kill pme, kill lock, etc.
3. Поискать аналоги (YDB, YT, PG).
4. Почему пользователям разрешено подключаться?
5. Является ли это отдельным СОСТОЯНИЕМ кластера?