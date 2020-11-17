using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Wenli.Data.Kafka.Common
{
    class SerializeUtil
    {
        /// <summary>
        ///     newton.json序列化,日志参数专用
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string Serialize(object obj)
        {
            JsonSerializerSettings settings = new JsonSerializerSettings();
            settings.ObjectCreationHandling = ObjectCreationHandling.Replace;
            settings.DateFormatString = "yyyy-MM-dd HH:mm:ss.fff";
            return JsonConvert.SerializeObject(obj, settings);
        }

        /// <summary>
        ///     newton.json反序列化,日志参数专用
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="json"></param>
        /// <returns></returns>
        public static T Deserialize<T>(string json)
        {
            JsonSerializerSettings settings = new JsonSerializerSettings();
            settings.ObjectCreationHandling = ObjectCreationHandling.Replace;
            settings.DateFormatString = "yyyy-MM-dd HH:mm:ss.fff";
            return JsonConvert.DeserializeObject<T>(json, settings);
        }

        /// <summary>
        ///     newton.json反序列化
        /// </summary>
        /// <param name="json"></param>
        /// <param name="type">反序列化的类型</param>
        /// <returns></returns>
        public static object Deserialize(string json, Type type)
        {
            JsonSerializerSettings settings = new JsonSerializerSettings();
            settings.ObjectCreationHandling = ObjectCreationHandling.Replace;
            settings.DateFormatString = "yyyy-MM-dd HH:mm:ss.fff";
            return JsonConvert.DeserializeObject(json, type, settings);
        }

        /// <summary>
        /// newton.json反序列化
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public static object Deserialize(string json)
        {
            JsonSerializerSettings settings = new JsonSerializerSettings();
            settings.ObjectCreationHandling = ObjectCreationHandling.Replace;
            settings.DateFormatString = "yyyy-MM-dd HH:mm:ss.fff";
            return JsonConvert.DeserializeObject(json, settings);
        }
        /// <summary>
        /// 通过JValue获取内部的value值
        /// </summary>
        /// <param name="jValue"></param>
        /// <returns></returns>
        public static T GetJsonValue<T>(object jValue)
        {
            var typedJVal = jValue as Newtonsoft.Json.Linq.JToken;

            if (typedJVal != null)
            {
                return typedJVal.ToObject<T>();
            }

            return default(T);
        }

        /// <summary>
        /// 通过JValue获取内部的value值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="jValue"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static T GetJsonValue<T>(object jValue, string key)
        {
            var typedJVal = jValue as Newtonsoft.Json.Linq.JToken;

            if (typedJVal != null)
            {
                return typedJVal.Value<T>(key);
            }

            return default(T);
        }

    }
}
