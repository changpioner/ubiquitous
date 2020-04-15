# ubiquitous

## About ubiquitous
    一个基于Spark streaming ,kafka , 缓存实现的延时队列插件。
    
    目前的项目中，有许多延时消费的需求，如15min后状态的确认，反洗日志时需要获取1小时后该用户登陆的用户名等。   
    
    目前流处理使用的是spark steaming + kafka，在流处理中如果将延时队列简单的以队列的形式放到内存中，基于spark fault tolerance会导致延时队列丢失或数据重复的情况，并且随着。
    
    为了避免数据丢失、重复以及不可控制的内存消耗，基于一个第三方缓存hbase,mysql,redis等实现延时队列功能。
    
    每条需要延迟消费的消息需要有主键作为record的key,为避免数据量大后的排序成本以一个时间轮保存这些key,为节省内存缓存表中保存key->value关系,当到达执行时间后，发送消息并在回调中删除缓存表中此条消息。
    
    以上逻辑保证了延迟消息的幂等性，由于spark streaming foreach partitions 中保证的是at least once,当任务重试或重启时能够保证消息的不丢失和不重复。