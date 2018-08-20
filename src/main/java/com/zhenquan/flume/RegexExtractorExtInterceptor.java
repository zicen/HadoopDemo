package com.zhenquan.flume;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.RegexExtractorInterceptorPassThroughSerializer;
import org.apache.flume.interceptor.RegexExtractorInterceptorSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class RegexExtractorExtInterceptor implements Interceptor {

  static final String REGEX = "regex";
  static final String SERIALIZERS = "serializers";

  private static final Logger logger = LoggerFactory
      .getLogger(RegexExtractorExtInterceptor.class);

  private final Pattern regex;
  private final List<NameAndSerializer> serializers;
  
  /***************start修改***************/
  //extractorHeader为true则提取header，为false则提取body
  static final String EXTRACTOR_HEADER = "extractorHeader";  
  //DEFAULT_EXTRACTOR_HEADER为默认值
  static final boolean DEFAULT_EXTRACTOR_HEADER = false;  
  //通过extractorHeaderKey抽取header中的文件名称，当extractorHeader为true时，则必须指定该参数。
  static final String EXTRACTOR_HEADER_KEY = "extractorHeaderKey";  
  
  
  private final boolean extractorHeader;  
  private final String extractorHeaderKey;  
 
  private RegexExtractorExtInterceptor(Pattern regex,
      List<NameAndSerializer> serializers,boolean extractorHeader, 
      String extractorHeaderKey) {
    this.regex = regex;
    this.serializers = serializers;
    this.extractorHeader = extractorHeader;  
    this.extractorHeaderKey = extractorHeaderKey; 
  }
  /****************end修改***************/
  public void initialize() {
    // NO-OP...
  }

  public void close() {
    // NO-OP...
  }
/**
 * 对Event进行拦截
 */
  public Event intercept(Event event) {
	  String tmpStr;  
	  //如果为true，提取header内容
      if(extractorHeader)  
      {  
    	  //根据extractorHeaderKey 去文件名称
          tmpStr = event.getHeaders().get(extractorHeaderKey);  
      }  
      else//如果为false，提取body内容
      {  
          tmpStr=new String(event.getBody(),  
                  Charsets.UTF_8);  
      }  
      //通过正则解析出a.log.2017-03-31
      Matcher matcher = regex.matcher(tmpStr); 
    //Matcher matcher = regex.matcher(new String(event.getBody(), Charsets.UTF_8));
    Map<String, String> headers = event.getHeaders();
    if (matcher.find()) {
      for (int group = 0, count = matcher.groupCount(); group < count; group++) {
        int groupIndex = group + 1;
        if (groupIndex > serializers.size()) {
          //debug调试
          if (logger.isDebugEnabled()) {
            logger.debug("Skipping group {} to {} due to missing serializer",
                group, count);
          }
          break;
        }
        NameAndSerializer serializer = serializers.get(group);
        //debug调试
        if (logger.isDebugEnabled()) {
          logger.debug("Serializing {} using {}", serializer.headerName,
              serializer.serializer);
        }
        //正则表达式提取的信息，分别赋值给s1.name s2.name s3.name
        headers.put(serializer.headerName,
            serializer.serializer.serialize(matcher.group(groupIndex)));
      }
    }
    return event;
  }
  /**
   * 返回Event集合，循环拦截单个Event
   */
  public List<Event> intercept(List<Event> events) {
    List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
    for (Event event : events) {
      Event interceptedEvent = intercept(event);
      if (interceptedEvent != null) {
        intercepted.add(interceptedEvent);
      }
    }
    return intercepted;
  }
  /**
   * 第一步执行
   * @author dajiangtai
   *
   */
  public static class Builder implements Interceptor.Builder {

    private Pattern regex;
    private List<NameAndSerializer> serializerList;
    
    /****************修改start***************/
    private boolean extractorHeader;  
    private String extractorHeaderKey; 
    /****************修改end***************/
    
    private final RegexExtractorInterceptorSerializer defaultSerializer =
        new RegexExtractorInterceptorPassThroughSerializer();

    public void configure(Context context) {
      //提取正则REGEX字符串
      String regexString = context.getString(REGEX);
      Preconditions.checkArgument(!StringUtils.isEmpty(regexString),
          "Must supply a valid regex string");
      regex = Pattern.compile(regexString);
      regex.pattern();
      regex.matcher("").groupCount();
      configureSerializers(context);
      
      /****************修改start***************/
      //提取extractorHeader true or false。如果未提取到，默认值为DEFAULT_EXTRACTOR_HEADER
      extractorHeader = context.getBoolean(EXTRACTOR_HEADER,  
              DEFAULT_EXTRACTOR_HEADER);  

      if (extractorHeader) {  
    	  //如果extractorHeader为true，获取extractorHeaderKey
          extractorHeaderKey = context.getString(EXTRACTOR_HEADER_KEY);  
          Preconditions.checkArgument(  
                  !StringUtils.isEmpty(extractorHeaderKey),  
                  "Must supply header key");  
      }  
      /****************修改end***************/
    }

    private void configureSerializers(Context context) {
      //通过参数serializers获取s1 s2 s3 字符串
      String serializerListStr = context.getString(SERIALIZERS);
      Preconditions.checkArgument(!StringUtils.isEmpty(serializerListStr),
          "Must supply at least one name and serializer");
      //将定义的s1 s2 s3 分割为数组
      String[] serializerNames = serializerListStr.split("\\s+");

      //得到serializers子属性的上下文context
      Context serializerContexts =
          new Context(context.getSubProperties(SERIALIZERS + "."));

      //构造一个List集合
      serializerList = Lists.newArrayListWithCapacity(serializerNames.length);
      for (String serializerName : serializerNames) {
    	//获取name子属性上下文context
        Context serializerContext = new Context(
            serializerContexts.getSubProperties(serializerName + "."));
        //获取type属性
        String type = serializerContext.getString("type", "DEFAULT");
        //获取name属性
        String name = serializerContext.getString("name");
        Preconditions.checkArgument(!StringUtils.isEmpty(name),
            "Supplied name cannot be empty.");
        //解析文件名称之后，分别赋值给s1 s2 s3
        if ("DEFAULT".equals(type)) {
          serializerList.add(new NameAndSerializer(name, defaultSerializer));
        } else {
          serializerList.add(new NameAndSerializer(name, getCustomSerializer(
              type, serializerContext)));
        }
      }
    }

    private RegexExtractorInterceptorSerializer getCustomSerializer(
        String clazzName, Context context) {
      try {
        RegexExtractorInterceptorSerializer serializer = (RegexExtractorInterceptorSerializer) Class
            .forName(clazzName).newInstance();
        serializer.configure(context);
        return serializer;
      } catch (Exception e) {
        logger.error("Could not instantiate event serializer.", e);
        Throwables.propagate(e);
      }
      return defaultSerializer;
    }

    public Interceptor build() {
      //检查regex正则是否配置错误
      Preconditions.checkArgument(regex != null,
          "Regex pattern was misconfigured");
      //检查serializerList(serializers=s1 s2 s3)是否有效
      Preconditions.checkArgument(serializerList.size() > 0,
          "Must supply a valid group match id list");
      //返回自定义拦截器对象
      return new RegexExtractorExtInterceptor(regex, serializerList,extractorHeader, extractorHeaderKey);
    }
  }

  /**
   * 封装每个serializer s1 s2 s3 的name和value
   * @author dajiangtai
   *
   */
  static class NameAndSerializer {
    private final String headerName;
    private final RegexExtractorInterceptorSerializer serializer;

    public NameAndSerializer(String headerName,
        RegexExtractorInterceptorSerializer serializer) {
      this.headerName = headerName;
      this.serializer = serializer;
    }
  }
}