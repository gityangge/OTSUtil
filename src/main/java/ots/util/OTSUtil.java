package ots.util;

import com.alicloud.openservices.tablestore.model.PutRowResponse;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;

import java.util.List;

public interface OTSUtil {
   <T> T serchByPrimaryKey(T t) throws Exception;

   <T> List<T> serchRange(Class<T> clz, RangeRowQueryCriteria rangeRowQueryCriteria) throws Exception;

   <T> PutRowResponse wirte(T t) throws Exception;

   <T> PutRowResponse wirteIfAbsent(T t) throws Exception;


}
