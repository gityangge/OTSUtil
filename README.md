# OTSUtil
用于支持阿里云表格存储(OTS)中间件的读写操作，主要功能是对参数的自动拆装。
欢迎建议，欢迎star

### 更新
* 2020-04-12 新增迭代器范围查询searchRangeByIterator，已解决普通范围查询内容不全问题

### 使用说明
@OTSClass(name="") 
>用于注解在表格存储数据类的头部，传入该类的标识

@OTSPrimaryKey(name="", type=OTSPrimaryKeyType.STRING)
>用于标注主键，必须与阿里云上的表格设计一致。type是数据类型,默认为STRING

@OTSColumn(name="", type=OTSColumnType.STRING)
>用于标注表格属性参数，type是数据类型，默认为STRING

@OTSChildClass
>用于标注子类型，只能标注于拥有@OTSClass的对象类型。

```
//写入一条记录
<T> PutRowResponse wirte(T t) throws Exception;
//不存在则写入
<T> PutRowResponse wirteIfAbsent(T t) throws Exception;
//范围查询，因为范围查询的可设置参数比较复杂，所以直接提供出criteria参数，请自行拼装
<T> List<T> serchRange(Class<T> clz, RangeRowQueryCriteria rangeRowQueryCriteria) throws Exception;
//搜索一条记录，注意需要将主键拼写完整，否则会报错
<T> T serchByPrimaryKey(T t) throws Exception;
```

### 配置阿里云Client
```
@Configuration
public class TSClient {
    private final String endPoint = "###";
    private final String accessKeyId = "###";
    private final String accessKeySecret = "###";
    private final String instanceName = "###";
    private SyncClient client = null;
    private OTSUtil otsUtil = null;

    @Bean
    public SyncClient tsClient() {
        if (client == null) {
            client = new SyncClient(endPoint, accessKeyId, accessKeySecret, instanceName);
        }
        return client;
    }

    @Bean
    public OTSUtil otsUtil() {
        if (otsUtil == null) {
            otsUtil = new OTSUtilImpl(client);
        }
        return otsUtil;
    }
}
```

### 快速使用
```
@OTSClass(name = "test_table")
    public class TestDomain{
        @OTSPrimaryKey(name = "name")
        String name;
        @OTSPrimaryKey(name = "value")
        String value;
        @OTSColumn(name = "Col1")
        String col1;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getCol1() {
            return col1;
        }

        public void setCol1(String col1) {
            this.col1 = col1;
        }
    }
    
    
    SyncClient client = getTsClient();
    OTSUtil otsUtil = new OTSUtilImpl(client);
    TestDomain testDomain = new TestDomain();
    testDomain.setName("new ID:0");
    testDomain.setValue("28e70caf-497c-48f6-9e4d-9d53deb29007");
    TestDomain data = otsUtil.serchByPrimaryKey(testDomain);
    System.err.println(data);
```