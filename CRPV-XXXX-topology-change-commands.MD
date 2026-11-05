# CRPV-XXXXX Topology change commands

## Задача

Предоставить администратору, в удобном виде, иформацию:

1. Распределение данных и процессах, которые будут происходить в кластере при измении топологии.
2. Серверных нодах, которые можно вывести из кластера без потери данных.

## Команда прогноза изменения топологии

Название команды `--toplogy-change --dry-run`

### 1. `--remove` 

Все флаги по умолчанию включены, если указан хотя бы 1 или несколько отдельных флагов то показываем информацию только из этих разделов.

#### Вход:
   * `--node|--nodes` - идентификатор существующих нод(ноды) которые будут удалены из кластера.
   * `--change-baseline` - если указан считаем результат исходя из того, что baseline будет изменен после выхода нод. 
      Не указан - расчитываем из параметров кластера (тут тоже может-быть включен auto adjust).
   * `--show-node-info` - показывать информацию об удаляемых нодах.
   * `--show-new-coordinator` - показывать информацию о новом координаторе кластера.
   * `--show-lost-partitions` - показывать информацию о потерянных партициях.
   * `--distribution-after` - показывать распределение партиций в кластере. если указан автоматически включает `--show-lost-partitions`
   * `--rebalance-info` - показывать информацию о том как будет выполнен ребаланс.
   * `--show-background-processes` - показывать информаци о долгих процессах, запущенных на удаляемых нодах.

#### Выход (порядок вывода соответствует порядку указания разделов):

Представлен примерный вывод команды.

* '#' в начале строки означет комментарий. Не попадает в фактический вывод команды.
* '*' в начале строки означает повторяющийся вывод для каждой сущност 
`* [cache-group-name]` - последовательно выводим каждую кеш-группу и всю вложенную информацию.

```
   # По флагу [--show-node-info]
   
   * [remove node id] - DC=[data center id],CELL=[cell id],(cluster coordinator|regular node).
   
   # По флагу [--show-new-coordinator]

   [alive node id] - new cluster coordinator.

   #По флагу [--show-lost-partitions]

   Список кешей которые потеряют данные (партиции)
   * [cache-group-name]
      LOST_PARTITIONS: [list of lost partitions]
      * [server-node]:
         PRIMARY: [list of primary partitions server node store]
         BACKUP: [list of backup partitions server node store]

   # По флагу [--rebalance-info]
   # Если baseline auto adjust - false и не указан флаг --change-baseline то пишем 

   No rebance because of settings: [здесь значения настроек которые привели к пропуску ребаланса]
   
   # Информация о ребалансе:

   * [server-node] # нода которая будет отдавать партици(supplier)
      * [cache-group-name]
         * [server-node] -> [partition list] # demander node -> downloaded partition list.

   # По флагу [--distribution-after]
   
   Список кешей которые НЕ потеряют данные(партиции) вида:
   * [cache-group-name] [backupAfter=[фактическое количество бекапов после изменения], backupCfg=[значение CacheConfiguration#backup]]
      * [server-node]:
         PRIMARY: [list of primary partitions server node store]
         BACKUP: [list of backup partitions server node store]

   # По флагу [--show-background-processes]
   # Список продолжительных задач запущенных на ноде: время начала и (прогнозируемым временем завершения - PHASE2):

   - перестроение индекса
   - снятие дампа/снапшота
   - дефрагментация
   - idle_verify
   - running services
   - ongoing rebalance
   - etc....
```


### 2. `--add` 

#### Вход:
   * `--node|--nodes` - идентификаторы нод(ноды) которые будут возвращены в кластер.
   * `--num-nodes` - количество НОВЫХ нод которые вводят в кластер.
   * `--change-baseline` - если указан считаем результат исходя из того, что baseline будет изменен после входа нод. 
      Не указан - расчитываем из параметров кластера (тут тоже может-быть включен auto adjust).
   * `--rebalance-info` - показывать информацию о том как будет выполнен ребаланс.
   * `--distrubtion-after` - показать информацию о распределении партций после ввода нод.

#### Выход:

Представлен примерный вывод команды.

* '#' в начале строки означет комментарий. Не попадает в фактический вывод команды.
* '*' в начале строки означает повторяющийся вывод для каждой сущност
  `* [cache-group-name]` - последовательно выводим каждую кеш-группу и всю вложенную информацию.

```
   # По флагу [--rebalance-info]
   # Если baseline auto adjust - false и не указан флаг --change-baseline то пишем 

   No rebance because of settings: [здесь значения настроек которые привели к пропуску ребаланса]

   # Информация о ребалансе:

   * [server-node] # нода которая будет отдавать партици(supplier)
      * [cache-group-name]
         * [server-node] -> [partition list] # demander node -> downloaded partition list.

  # По флагу [--distribution-after]
       * [cache-group-name] [backupAfter=[фактическое количество бекапов после изменения], backupCfg=[значение CacheConfiguration#backup]]
           * [server-node]:
               PRIMARY: [list of primary partitions server node store]
               BACKUP: [list of backup partitions server node store]
```

## Команда безопасного удаления серверных нод

Название команды `--topology-change --no-data-lost-if-remove`

#### Вход
   * `--no-backup ignore|warning`
      * Если `ignore` - исключить кеши без бекапов из расчета.
      * Если `warning` - исключить кеши без бекапов из расчета, но вывести отдельным списком.
   * `--skip-no-cell-caches` - Если в кластере настроены ячейки, не учитывать кеши в которые бекап-фильтр сконфигурирован без ячеек.

#### Алгоритм

* (?) В случае использования ячеек и все кеши настроены с ячеистой аффинити
  * Можно удалить все `size(cell) - 1` серверов из каждой ячейки.
  ```java
    List<UUID> generateSafeToRemoveFromCell(List<UUID> cell) {
        List<List<UUID>> res = new ArrayList();
        for (int i=0; i<cell.size; i++) {
            List<UUID> canRmv = new ArrayList<>(cell);
            canRmv.remove(i);
            res.add(canRmv);
        }
        return res;
    }
  
    List<List<UUID>> addNodes(List<List<UUID>> cells) {
        List<List<UUID>> res = new ArrayList();
  
        for (List<UUID> cell: cells) {
           List<List<UUID>> safeToRemoveFromCell = generateSafeToRemoveFromCell(cell);
     
           if (safeToRemoveFromCell.isEmpty()) 
                continue;

           if (res.isEmpty()) 
                for (List<UUID> canRmv: safeToRemoveFromCell)
                    res.addAll(canRmv);
           else {
                for (List<UUID> canRmv: safeToRemoveFromCell)
                    for (List<UUID> singleRes: res)
                        singleRes.addAll(safeToRemoveFromCell);
           }
        }
    }

  ```
* В случае обычного кластера:
   * Определяем максимальное число серверных нод которые можно вывести из кластера.
     Оно равняется минимальному количеству бекапов среди всех `PARTITIONED` кешей кластера:
  ```java
  int maxNodes = cluster.size() - 1;
  for (CacheConfiguration ccfg : caches) {
     if (ccfg.getMode() == REPLICATED)
        continue;
  
     if (hideCachesWithoutBackup && ccfg.getBackup() == 0)
         continue;
  
     maxNodes = Math.min(ccfg.getBackup(), maxNodes);
  }
  
  return maxNodes;
  ```
   * Берем все выборки серверных нод кластера состоящих из `maxNodes` элементов.
     Расчитываем доступность всех партиций всех кешей при условии удаления этой выборки из кластера.
     Если некоторые партиции недоступны - пропуск.
     Если все партиции доступны - добавляем выборку в результат.
     Если не найдено ни одной выборки то повторяем с `maxNodes - 1`. 

#### Выход

```
   # Набор серверов которые можно удалить.

   SET_[X]
      * [server-node]

```

## Команда состава ячеек

Для контроля использования в кластере ячеистой аффинити необходима команда, которая выводит информацию конкретно для этой настройки.

Название команды `--cells`

#### Вход
   * `--cache-info` - если указано добавляем информацию о кешах.

#### Выход

Представлен примерный вывод команды.

* '#' в начале строки означет комментарий. Не попадает в фактический вывод команды.
* '*' в начале строки означает повторяющийся вывод для каждой сущност
  `* [cache-group-name]` - последовательно выводим каждую кеш-группу и всю вложенную информацию.

```
   # X - идентификатор ячейки. Вывод упорядочек по X по возрастанию.

   * CELL_[X]
      # Если настроен DC то выводи информаци сгрупированную по нему. Иначе список нод.

      * DC_[Y]
         * [server_node_id]
         
   # Только если указано `--cache-info`.

   cache info:
   
   # Список групп, которые НЕ используют ячеистый backup filter.
   
   OTHER_AFFINITY:
      * [cache_group_name]
   
   # Список групп, которые используют ячеистый backup filter.

   CELL_AFFINITY:
      * [cache_group_name]
```

## Команда которая отобразит подробный статус ребаланса

Откуда и куда мы скачиваем партиции прямо сейчас.