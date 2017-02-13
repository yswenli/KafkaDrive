/*****************************************************************************************************
 * 本代码版权归@wenli所有，All Rights Reserved (C) 2015-2017
 *****************************************************************************************************
 * CLR版本：4.0.30319.42000
 * 唯一标识：2003103a-3951-4ad4-b29a-23c2afb84ccc
 * 机器名称：WENLI-PC
 * 联系人邮箱：wenguoli_520@qq.com
 *****************************************************************************************************
 * 项目名称：$projectname$
 * 命名空间：Wenli.Drive.Kafka
 * 类名称：KafkaConfig
 * 创建时间：2017/2/10 16:51:47
 * 创建人：wenli
 * 创建说明：
 *****************************************************************************************************/
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Wenli.Drive.Kafka
{
    /// <summary>
    /// kafka配置类
    /// </summary>
    public class KafkaConfig : ConfigurationSection
    {

        /// <summary>
        ///     当前配置名称
        ///     此属性为必须
        /// </summary>
        public string SectionName
        {
            get; set;
        }
        /// <summary>
        ///     代理
        /// </summary>
        [ConfigurationProperty("broker", IsRequired = true)]
        public string Broker
        {
            get
            {
                return (string)base["broker"];
            }
            set
            {
                base["broker"] = value;
            }
        }
        /// <summary>
        /// 主题
        /// </summary>
        [ConfigurationProperty("topic", IsRequired = true)]
        public string Topic
        {
            get
            {
                return (string)base["topic"];
            }
            set
            {
                base["topic"] = value;
            }
        }

        #region 从配置文件中创建kafka配置类

        /// <summary>
        ///     获取默认kafka配置类
        /// </summary>
        /// <returns></returns>
        public static KafkaConfig GetConfig()
        {
            return (KafkaConfig)ConfigurationManager.GetSection("kafkaConfig");
        }

        /// <summary>
        ///     获取指定的kafka配置类
        /// </summary>
        /// <param name="sectionName"></param>
        /// <returns></returns>
        public static KafkaConfig GetConfig(string sectionName)
        {
            var section = (KafkaConfig)ConfigurationManager.GetSection(sectionName);
            //  跟默认配置相同的，可以省略
            if (section == null)
                section = GetConfig();
            if (section == null)
                throw new ConfigurationErrorsException("kafkacofng节点 " + sectionName + " 未配置.");
            section.SectionName = sectionName;
            return section;
        }

        /// <summary>
        ///     从指定位置读取配置
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="sectionName"></param>
        /// <returns></returns>
        public static KafkaConfig GetConfig(string fileName, string sectionName)
        {
            return GetConfig(ConfigurationManager.OpenMappedMachineConfiguration(new ConfigurationFileMap(fileName)),
                sectionName);
        }

        /// <summary>
        ///     从指定Configuration中读取配置
        /// </summary>
        /// <param name="config"></param>
        /// <param name="sectionName"></param>
        /// <returns></returns>
        public static KafkaConfig GetConfig(Configuration config, string sectionName)
        {
            if (config == null)
                throw new ConfigurationErrorsException("传入的配置不能为空");
            var section = (KafkaConfig)config.GetSection(sectionName);
            if (section == null)
                throw new ConfigurationErrorsException("kafkacofng节点 " + sectionName + " 未配置.");
            section.SectionName = sectionName;
            return section;
        }

        #endregion
    }
}
