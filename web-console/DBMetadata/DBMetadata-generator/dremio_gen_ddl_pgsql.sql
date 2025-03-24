CREATE TABLE IF NOT EXISTS "麻醉记录" (
"医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "麻醉医师工号" varchar (20) DEFAULT NULL,
 "麻醉医师姓名" varchar (50) DEFAULT NULL,
 "患者去向代码" varchar (1) DEFAULT NULL,
 "出血量(mL)" decimal (5,
 0) DEFAULT NULL,
 "输血时间" timestamp DEFAULT NULL,
 "输血反应标志" varchar (1) DEFAULT NULL,
 "输血量计量单位" varchar (10) DEFAULT NULL,
 "输血量(mL)" decimal (4,
 0) DEFAULT NULL,
 "输血品种代码" varchar (10) DEFAULT NULL,
 "术中输液项目" varchar (50) DEFAULT NULL,
 "手术者姓名" varchar (50) DEFAULT NULL,
 "出手术室时间" timestamp DEFAULT NULL,
 "手术结束时间" timestamp DEFAULT NULL,
 "麻醉开始时间" timestamp DEFAULT NULL,
 "手术开始时间" timestamp DEFAULT NULL,
 "麻醉前用药" varchar (100) DEFAULT NULL,
 "麻醉效果" varchar (100) DEFAULT NULL,
 "ASA分级代码" decimal (1,
 0) DEFAULT NULL,
 "穿刺过程" text,
 "诊疗过程描述" text,
 "麻醉合并症标志" varchar (1) DEFAULT NULL,
 "特殊监测项目结果" varchar (200) DEFAULT NULL,
 "特殊监测项目名称" varchar (200) DEFAULT NULL,
 "常规监测项目结果" varchar (200) DEFAULT NULL,
 "常规监测项目名称" varchar (100) DEFAULT NULL,
 "麻醉描述" varchar (200) DEFAULT NULL,
 "呼吸类型代码" varchar (1) DEFAULT NULL,
 "麻醉体位" varchar (100) DEFAULT NULL,
 "麻醉药物名称" varchar (250) DEFAULT NULL,
 "气管插管分类" varchar (100) DEFAULT NULL,
 "麻醉方法代码" varchar (20) DEFAULT NULL,
 "手术体位代码" varchar (3) DEFAULT NULL,
 "手术及操作代码" varchar (20) DEFAULT NULL,
 "术后诊断代码" varchar (64) DEFAULT NULL,
 "术前诊断代码" varchar (100) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "手术间编号" varchar (20) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "手术明细流水号" varchar (32) DEFAULT NULL,
 "电子申请单编号" varchar (100) DEFAULT NULL,
 "麻醉术前访视记录单号" varchar (64) NOT NULL,
 "门诊就诊流水号" varchar (32) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "麻醉记录流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 CONSTRAINT "麻醉记录"_"医疗机构代码"_"麻醉术前访视记录单号"_"麻醉记录流水号"_PK PRIMARY KEY ("医疗机构代码",
 "麻醉术前访视记录单号",
 "麻醉记录流水号")
);
COMMENT ON COLUMN "麻醉记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "麻醉记录"."麻醉记录流水号" IS '按照特定编码规则赋予麻醉记录的唯一标志';
COMMENT ON COLUMN "麻醉记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "麻醉记录"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "麻醉记录"."门诊就诊流水号" IS '为门诊就诊时，填写门诊就诊流水号，住院就诊流水号填“-”';
COMMENT ON COLUMN "麻醉记录"."麻醉术前访视记录单号" IS '按照特定编码规则赋予麻醉术前访视记录的唯一标志';
COMMENT ON COLUMN "麻醉记录"."电子申请单编号" IS '按照某一特定编码规则赋予电子申请单的顺序号';
COMMENT ON COLUMN "麻醉记录"."手术明细流水号" IS '按照特定编码规则赋予一个手术(操作)唯一标志的顺序号';
COMMENT ON COLUMN "麻醉记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "麻醉记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "麻醉记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "麻醉记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "麻醉记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "麻醉记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "麻醉记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "麻醉记录"."手术间编号" IS '对患者实施手术操作时所在的手术室房间工号';
COMMENT ON COLUMN "麻醉记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "麻醉记录"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "麻醉记录"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "麻醉记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "麻醉记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "麻醉记录"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "麻醉记录"."ABO血型代码" IS '为患者实际输入的ABO血型类别在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉记录"."Rh血型代码" IS '为患者实际输入的Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉记录"."术前诊断代码" IS '术前诊断在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉记录"."术后诊断代码" IS '术后诊断在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉记录"."手术及操作代码" IS '手术及操作在特定编码体系中的唯一标识';
COMMENT ON COLUMN "麻醉记录"."手术体位代码" IS '手术时为患者采取的体位(如仰卧位、俯卧位、左侧卧位、截石位、屈氏位等)在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉记录"."麻醉方法代码" IS '为患者进行手术、操作时使用的麻醉方法在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉记录"."气管插管分类" IS '标识全身麻醉时气管插管分类的描述';
COMMENT ON COLUMN "麻醉记录"."麻醉药物名称" IS '平台中心麻醉药物名称';
COMMENT ON COLUMN "麻醉记录"."麻醉体位" IS '麻醉体位的详细描述';
COMMENT ON COLUMN "麻醉记录"."呼吸类型代码" IS '呼吸类型(如自主呼吸、辅助呼吸、控制呼吸)在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉记录"."麻醉描述" IS '对患者麻醉过程的详细描述';
COMMENT ON COLUMN "麻醉记录"."常规监测项目名称" IS '麻醉过程中，需要常规监测项目的名称';
COMMENT ON COLUMN "麻醉记录"."常规监测项目结果" IS '麻醉过程中，常规监测项目结果的详细记录';
COMMENT ON COLUMN "麻醉记录"."特殊监测项目名称" IS '麻醉过程中，需要特殊监测项目的名称';
COMMENT ON COLUMN "麻醉记录"."特殊监测项目结果" IS '麻醉过程中，特殊监测项目结果的详细记录';
COMMENT ON COLUMN "麻醉记录"."麻醉合并症标志" IS '标识是否具有麻醉合并症的标志';
COMMENT ON COLUMN "麻醉记录"."诊疗过程描述" IS '对患者诊疗过程或抢救情况的详细描述';
COMMENT ON COLUMN "麻醉记录"."穿刺过程" IS '局部麻醉中穿刺过程的详细描述';
COMMENT ON COLUMN "麻醉记录"."ASA分级代码" IS '根据美同麻醉师协会(ASA)制定的分级标准，对病人体质状况和对手术危险性进行评估分级的结果在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉记录"."麻醉效果" IS '实施麻醉效果的描述';
COMMENT ON COLUMN "麻醉记录"."麻醉前用药" IS '对患者进行麻醉前给予的药品的具体描述';
COMMENT ON COLUMN "麻醉记录"."手术开始时间" IS '以手术操作刀碰皮(切皮)时点计算，经自然腔道到达手术部位，没有皮肤切口的手术，开始时间以手术器械进入人体腔道的时点为准,对患者开始手术操作时的公元纪年日期和时间的完整描述。';
COMMENT ON COLUMN "麻醉记录"."麻醉开始时间" IS '麻醉开始时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉记录"."手术结束时间" IS '对患者结束手术操作时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉记录"."出手术室时间" IS '患者离开手术室时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉记录"."手术者姓名" IS '实施手术的主要执行人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "麻醉记录"."术中输液项目" IS '手术过程中输入液体的描述';
COMMENT ON COLUMN "麻醉记录"."输血品种代码" IS '输入全血或血液成分类别(如红细胞、全血、血小板、血浆等)在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉记录"."输血量(mL)" IS '输入红细胞、血小板、血浆、全血等的数量，剂量单位为mL';
COMMENT ON COLUMN "麻醉记录"."输血量计量单位" IS '血液或血液成分的计量单位的机构内名称，如治疗量、U、ML等';
COMMENT ON COLUMN "麻醉记录"."输血反应标志" IS '标志患者术中输血后是否发生了输血反应的标志';
COMMENT ON COLUMN "麻醉记录"."输血时间" IS '患者开始进行输血时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉记录"."出血量(mL)" IS '手术过程的总出血量，单位ml';
COMMENT ON COLUMN "麻醉记录"."患者去向代码" IS '患者当前诊疗过程结束后的去向在特定编码体系中的代码，这里指离开急诊观察室或监护室后的去向';
COMMENT ON COLUMN "麻醉记录"."麻醉医师姓名" IS '对患者实施麻醉的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "麻醉记录"."麻醉医师工号" IS '麻醉医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "麻醉记录"."签名时间" IS '麻醉医师完成签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "麻醉记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON TABLE "麻醉记录" IS '麻醉相关信息，包括麻醉方法、体位以及用药、输血信息';


CREATE TABLE IF NOT EXISTS "麻醉术后访视记录" (
"一般状况检查结果" text,
 "术后诊断代码" varchar (64) DEFAULT NULL,
 "术前诊断代码" varchar (64) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "电子申请单编号" varchar (100) DEFAULT NULL,
 "麻醉记录流水号" varchar (64) DEFAULT NULL,
 "门诊就诊流水号" varchar (32) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "麻醉术后访视记录流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "更新时间" timestamp DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "麻醉医师姓名" varchar (50) DEFAULT NULL,
 "麻醉医师工号" varchar (20) DEFAULT NULL,
 "麻醉适应证" varchar (100) DEFAULT NULL,
 "特殊情况" text,
 "拔除气管插管标志" varchar (1) DEFAULT NULL,
 "清醒时间" timestamp DEFAULT NULL,
 "麻醉恢复情况" varchar (100) DEFAULT NULL,
 "麻醉方法代码" varchar (20) DEFAULT NULL,
 "手术及操作代码" varchar (20) DEFAULT NULL,
 CONSTRAINT "麻醉术后访视记录"_"麻醉术后访视记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("麻醉术后访视记录流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "麻醉术后访视记录"."手术及操作代码" IS '手术及操作在特定编码体系中的唯一标识';
COMMENT ON COLUMN "麻醉术后访视记录"."麻醉方法代码" IS '为患者进行手术、操作时使用的麻醉方法在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术后访视记录"."麻醉恢复情况" IS '对麻醉恢复情况的描述';
COMMENT ON COLUMN "麻醉术后访视记录"."清醒时间" IS '麻醉后患者清醒时的公元纪年0期和时间的完整描述';
COMMENT ON COLUMN "麻醉术后访视记录"."拔除气管插管标志" IS '标识是否已拔除气管插管的标志';
COMMENT ON COLUMN "麻醉术后访视记录"."特殊情况" IS '对存在特殊情况的描述';
COMMENT ON COLUMN "麻醉术后访视记录"."麻醉适应证" IS '麻醉适应证的描述';
COMMENT ON COLUMN "麻醉术后访视记录"."麻醉医师工号" IS '麻醉医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "麻醉术后访视记录"."麻醉医师姓名" IS '对患者实施麻醉的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "麻醉术后访视记录"."签名时间" IS '麻醉医师完成签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉术后访视记录"."更新时间" IS '手术知情同意书内容更新完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉术后访视记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "麻醉术后访视记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉术后访视记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉术后访视记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "麻醉术后访视记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "麻醉术后访视记录"."麻醉术后访视记录流水号" IS '按照特定编码规则赋予麻醉术后访视记录的唯一标志';
COMMENT ON COLUMN "麻醉术后访视记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "麻醉术后访视记录"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术后访视记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "麻醉术后访视记录"."门诊就诊流水号" IS '为门诊就诊时，填写门诊就诊流水号，住院就诊流水号填“-”';
COMMENT ON COLUMN "麻醉术后访视记录"."麻醉记录流水号" IS '按照特定编码规则赋予麻醉记录的唯一标志';
COMMENT ON COLUMN "麻醉术后访视记录"."电子申请单编号" IS '按照某一特定编码规则赋予电子申请单的顺序号';
COMMENT ON COLUMN "麻醉术后访视记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "麻醉术后访视记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "麻醉术后访视记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "麻醉术后访视记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术后访视记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "麻醉术后访视记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "麻醉术后访视记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "麻醉术后访视记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "麻醉术后访视记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "麻醉术后访视记录"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术后访视记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "麻醉术后访视记录"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "麻醉术后访视记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "麻醉术后访视记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "麻醉术后访视记录"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "麻醉术后访视记录"."ABO血型代码" IS '为患者实际输入的ABO血型类别在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术后访视记录"."Rh血型代码" IS '为患者实际输入的Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术后访视记录"."术前诊断代码" IS '术前诊断在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术后访视记录"."术后诊断代码" IS '术后诊断在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术后访视记录"."一般状况检查结果" IS '对患者一般状况检査结果的详细描述，包括其发育状况、营养状况、体味、步态、面容与表情、意识，检查能否合作等';
COMMENT ON TABLE "麻醉术后访视记录" IS '术后方式记录，包括麻醉恢复情况、麻醉该方法、手术信息等';


CREATE TABLE IF NOT EXISTS "麻醉体征监测记录" (
"数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "呼吸频率(次/min)" decimal (3,
 0) DEFAULT NULL,
 "脉率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "心率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "监测时间" timestamp DEFAULT NULL,
 "麻醉记录流水号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "监测明细流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "麻醉体征监测记录"_"医疗机构代码"_"监测明细流水号"_PK PRIMARY KEY ("医疗机构代码",
 "监测明细流水号")
);
COMMENT ON COLUMN "麻醉体征监测记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉体征监测记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "麻醉体征监测记录"."监测明细流水号" IS '按照特定编码规则赋予监测明细记录的唯一标志';
COMMENT ON COLUMN "麻醉体征监测记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "麻醉体征监测记录"."麻醉记录流水号" IS '按照特定编码规则赋予麻醉记录的唯一标志';
COMMENT ON COLUMN "麻醉体征监测记录"."监测时间" IS '数据监测完成时的公元纪年和日期的完整描述';
COMMENT ON COLUMN "麻醉体征监测记录"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "麻醉体征监测记录"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "麻醉体征监测记录"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "麻醉体征监测记录"."心率(次/min)" IS '心脏搏动频率的测量值,计量单位为次/min';
COMMENT ON COLUMN "麻醉体征监测记录"."脉率(次/min)" IS '每分钟脉搏的次数测量值，计量单位为次/min';
COMMENT ON COLUMN "麻醉体征监测记录"."呼吸频率(次/min)" IS '受检者单位时间内呼吸的次数，计量单位为次/min';
COMMENT ON COLUMN "麻醉体征监测记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "麻醉体征监测记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "麻醉体征监测记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON TABLE "麻醉体征监测记录" IS '麻醉体征监测记录，包括监测时间、收缩压、舒张压等';


CREATE TABLE IF NOT EXISTS "首次病程记录" (
"初步诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "初步诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "初步诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "初步诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "鉴别诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "鉴别诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "鉴别诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "鉴别诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "鉴别诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "鉴别诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "住院医师工号" varchar (20) DEFAULT NULL,
 "住院医师姓名" varchar (50) DEFAULT NULL,
 "上级医师工号" varchar (20) DEFAULT NULL,
 "上级医师姓名" varchar (50) DEFAULT NULL,
 "文本内容" text,
 "密级" varchar (16) DEFAULT NULL,
 "初步诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "住院就诊流水号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "病程记录流水号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "记录时间" timestamp DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "主诉" text,
 "病例特点" text,
 "诊断依据" varchar (100) DEFAULT NULL,
 "诊疗计划" text,
 "中医“四诊”观察结果" text,
 "治则治法代码" varchar (100) DEFAULT NULL,
 "初步诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 CONSTRAINT "首次病程记录"_"医疗机构代码"_"住院就诊流水号"_PK PRIMARY KEY ("医疗机构代码",
 "住院就诊流水号")
);
COMMENT ON COLUMN "首次病程记录"."初步诊断-西医诊断代码" IS '按照平台特定编码规则赋予西医初步诊断疾病的唯一标识';
COMMENT ON COLUMN "首次病程记录"."治则治法代码" IS '辩证结果采用的治则治法在特定编码体系中的代码。如有多条，用“，”加以分隔';
COMMENT ON COLUMN "首次病程记录"."中医“四诊”观察结果" IS '中医“四诊”观察结果的详细描述，包括望、闻、问、切四诊内容';
COMMENT ON COLUMN "首次病程记录"."诊疗计划" IS '具体的检査、中西医治疗措施及中医调护';
COMMENT ON COLUMN "首次病程记录"."诊断依据" IS '对疾病诊断依据的详细描述';
COMMENT ON COLUMN "首次病程记录"."病例特点" IS '对病史、体格检查和辅助检查进行全面分析、归纳和整理后写出的本病例特征，包括阳性发现和具有鉴别意义的阴性症状和体征等';
COMMENT ON COLUMN "首次病程记录"."主诉" IS '对患者本次疾病相关的主要症状及其持续时间的描述，一般由患者本人或监护人描述';
COMMENT ON COLUMN "首次病程记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "首次病程记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "首次病程记录"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "首次病程记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "首次病程记录"."性别名称" IS '一般指患者医学生理性别，指男性或女性';
COMMENT ON COLUMN "首次病程记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "首次病程记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "首次病程记录"."记录时间" IS '完成首次病程记录的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "首次病程记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "首次病程记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "首次病程记录"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "首次病程记录"."科室名称" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "首次病程记录"."科室代码" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "首次病程记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "首次病程记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "首次病程记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "首次病程记录"."住院次数" IS '办理完整住院治疗手续的次数';
COMMENT ON COLUMN "首次病程记录"."病程记录流水号" IS '按照某一特定编码规则赋予病程记录的唯一标识';
COMMENT ON COLUMN "首次病程记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "首次病程记录"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "首次病程记录"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名';
COMMENT ON COLUMN "首次病程记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "首次病程记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "首次病程记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "首次病程记录"."初步诊断-中医病名代码" IS '按照平台特定编码规则赋予初步诊断中医疾病的唯一标识';
COMMENT ON COLUMN "首次病程记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "首次病程记录"."文本内容" IS '存入大文本的内容，最大不超过64K';
COMMENT ON COLUMN "首次病程记录"."上级医师姓名" IS '上级审核医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "首次病程记录"."上级医师工号" IS '上级审核医师的工号';
COMMENT ON COLUMN "首次病程记录"."住院医师姓名" IS '所在科室具体负责诊治的，具有住院医师专业技术职务任职资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "首次病程记录"."住院医师工号" IS '所在科室具体负责诊治的，具有住院医师的工号';
COMMENT ON COLUMN "首次病程记录"."鉴别诊断-中医症候名称" IS '鉴别诊断中医证候特定编码体系中的名称';
COMMENT ON COLUMN "首次病程记录"."鉴别诊断-中医症候代码" IS '按照平台编码规则赋予鉴别诊断中医证候的唯一标识';
COMMENT ON COLUMN "首次病程记录"."鉴别诊断-中医病名名称" IS '中医鉴别诊断在特定编码体系中的名称';
COMMENT ON COLUMN "首次病程记录"."鉴别诊断-中医病名代码" IS '按照平台特定编码规则赋予中医鉴别诊断的唯一标识';
COMMENT ON COLUMN "首次病程记录"."鉴别诊断-西医诊断名称" IS '西医鉴别诊断在特定编码体系中的名称';
COMMENT ON COLUMN "首次病程记录"."鉴别诊断-西医诊断代码" IS '按照平台编码规则赋予西医鉴别诊断的唯一标识';
COMMENT ON COLUMN "首次病程记录"."初步诊断-中医症候名称" IS '由医师根据患者人院时的情况，综合分析所作出的标准中医证候名称';
COMMENT ON COLUMN "首次病程记录"."初步诊断-中医症候代码" IS '按照平台特定编码规则赋予初步诊断中医证候的唯一标识';
COMMENT ON COLUMN "首次病程记录"."初步诊断-中医病名名称" IS '由医师根据患者人院时的情况，综合分析所作出的中医疾病标准名称';
COMMENT ON COLUMN "首次病程记录"."初步诊断-西医诊断名称" IS '由医师根据患者入院时的情况，综合分析所作出的西医诊断标准名称';
COMMENT ON TABLE "首次病程记录" IS '患者入院后第一次写的病情评估结果，包括诊断信息和医师信息';


CREATE TABLE IF NOT EXISTS "预防接种卡" (
"接种禁忌" varchar (1000) DEFAULT NULL,
 "疫苗异常反应史" varchar (1000) DEFAULT NULL,
 "迁入日期" date DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "接种流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "预防接种卡编号" varchar (20) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "城乡居民健康档案编号" varchar (17) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "监护人姓名" varchar (50) DEFAULT NULL,
 "监护人与本人关系代码" varchar (2) DEFAULT NULL,
 "监护人与本人关系名称" varchar (50) DEFAULT NULL,
 "本人电话号码" varchar (20) DEFAULT NULL,
 "家人电话号码" varchar (20) DEFAULT NULL,
 "工作单位电话号码" varchar (20) DEFAULT NULL,
 "邮政编码" varchar (6) DEFAULT NULL,
 "现住详细地址" varchar (128) DEFAULT NULL,
 "现住址行政区划代码" varchar (12) DEFAULT NULL,
 "现住址-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "现住址-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "现住址-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "现住址-县(市、区)代码" varchar (20) DEFAULT NULL,
 "现住址-县(市、区)名称" varchar (70) DEFAULT NULL,
 "现地址-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "现住址-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "现住址-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "现住址-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "现住址-门牌号码" varchar (70) DEFAULT NULL,
 "户籍地-详细地址" varchar (200) DEFAULT NULL,
 "户籍地-行政区划代码" varchar (12) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "户籍地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "户籍地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "户籍地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "户籍地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "户籍地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "户籍地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "户籍地-门牌号码" varchar (100) DEFAULT NULL,
 "迁出日期" date DEFAULT NULL,
 "迁出原因" varchar (100) DEFAULT NULL,
 "现住址-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "预防接种后不良反应临床诊断名称" varchar (200) DEFAULT NULL,
 "预防接种后不良反应临床诊断代码" varchar (4) DEFAULT NULL,
 "预防接种后不良反应发生日期" date DEFAULT NULL,
 "预防接种后不良反应处理结果" varchar (1000) DEFAULT NULL,
 "引起预防接种后不良反应的可疑疫苗名称代码" varchar (4) DEFAULT NULL,
 "疫苗接种单位名称" varchar (70) DEFAULT NULL,
 "疫苗接种单位代码" varchar (22) DEFAULT NULL,
 "建卡日期" date DEFAULT NULL,
 "建卡人姓名" varchar (50) DEFAULT NULL,
 "建卡人工号" varchar (20) DEFAULT NULL,
 "疫苗接种医生姓名" varchar (50) DEFAULT NULL,
 "疫苗接种医生工号" varchar (20) DEFAULT NULL,
 "剂次" decimal (2,
 0) DEFAULT NULL,
 "接种批次" decimal (1,
 0) DEFAULT NULL,
 "疫苗追溯码" varchar (200) DEFAULT NULL,
 "疫苗批号" varchar (30) DEFAULT NULL,
 "接种属性代码" varchar (1) DEFAULT NULL,
 "接种部位" varchar (100) DEFAULT NULL,
 "疫苗接种日期" date DEFAULT NULL,
 "疫苗费用类型" varchar (1) DEFAULT NULL,
 "疫苗生产厂商名称" varchar (200) DEFAULT NULL,
 "疫苗生产厂商代码" varchar (64) DEFAULT NULL,
 "疫苗主要成分代码" varchar (100) DEFAULT NULL,
 "疫苗有效期截止日期" date DEFAULT NULL,
 "疫苗名称" varchar (50) DEFAULT NULL,
 "疫苗代码" varchar (4) DEFAULT NULL,
 "传染病史" text,
 CONSTRAINT "预防接种卡"_"医疗机构代码"_"接种流水号"_PK PRIMARY KEY ("医疗机构代码",
 "接种流水号")
);
COMMENT ON COLUMN "预防接种卡"."传染病史" IS '患者既往所患各种急性或慢性传染性疾病名称的详细描述';
COMMENT ON COLUMN "预防接种卡"."疫苗代码" IS '接种者注射疫苗在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."疫苗名称" IS '接种者注射疫苗在特定编码体系中名称';
COMMENT ON COLUMN "预防接种卡"."疫苗有效期截止日期" IS '疫苗有效期截止日期';
COMMENT ON COLUMN "预防接种卡"."疫苗主要成分代码" IS '疫苗主要成分描述';
COMMENT ON COLUMN "预防接种卡"."疫苗生产厂商代码" IS '疫苗生产企业在工商局注册、审批通过后的企业在机构内编码体系中的编号';
COMMENT ON COLUMN "预防接种卡"."疫苗生产厂商名称" IS '疫苗生产企业在工商局注册、审批通过后的厂家名称';
COMMENT ON COLUMN "预防接种卡"."疫苗费用类型" IS '疫苗费用类型的描述，1、自费；2、免费';
COMMENT ON COLUMN "预防接种卡"."疫苗接种日期" IS '患者接种疫苗的公元纪年日期';
COMMENT ON COLUMN "预防接种卡"."接种部位" IS '疫苗接种部位描述';
COMMENT ON COLUMN "预防接种卡"."接种属性代码" IS '预防接种属性在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."疫苗批号" IS '接种疫苗的批号';
COMMENT ON COLUMN "预防接种卡"."疫苗追溯码" IS '药品包装上用来药品追溯及其相关信息做的标识';
COMMENT ON COLUMN "预防接种卡"."接种批次" IS '疫苗的生产批号及有效期属性';
COMMENT ON COLUMN "预防接种卡"."剂次" IS '本人接种某种疫苗的次数，计量单位为次';
COMMENT ON COLUMN "预防接种卡"."疫苗接种医生工号" IS '疫苗接种医师的工号';
COMMENT ON COLUMN "预防接种卡"."疫苗接种医生姓名" IS '疫苗接种医师在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "预防接种卡"."建卡人工号" IS '建卡人的工号';
COMMENT ON COLUMN "预防接种卡"."建卡人姓名" IS '建卡人在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "预防接种卡"."建卡日期" IS '建立疫苗接种卡当日的公元纪年日期';
COMMENT ON COLUMN "预防接种卡"."疫苗接种单位代码" IS '按照某一特定编码规则赋予疫苗接种单位的唯一标识。这里指医疗机构组织机构代码';
COMMENT ON COLUMN "预防接种卡"."疫苗接种单位名称" IS '疫苗接种单位的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "预防接种卡"."引起预防接种后不良反应的可疑疫苗名称代码" IS '引起预防接种后不良反应的可疑疫苗在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."预防接种后不良反应处理结果" IS '对于预防接种后不良反应处理的详细描述';
COMMENT ON COLUMN "预防接种卡"."预防接种后不良反应发生日期" IS '预防接种后不良反应发生当日的公元纪年日期';
COMMENT ON COLUMN "预防接种卡"."预防接种后不良反应临床诊断代码" IS '预防接种后不良反应临床诊断对应的平台中心诊断代码';
COMMENT ON COLUMN "预防接种卡"."预防接种后不良反应临床诊断名称" IS '预防接种后不良反应临床诊断对应的平台中心诊断名称';
COMMENT ON COLUMN "预防接种卡"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "预防接种卡"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "预防接种卡"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "预防接种卡"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "预防接种卡"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "预防接种卡"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "预防接种卡"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "预防接种卡"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "预防接种卡"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "预防接种卡"."现住址-省(自治区、直辖市)名称" IS '接种者现住地址中的省、自治区或直辖市名称';
COMMENT ON COLUMN "预防接种卡"."迁出原因" IS '迁出管理机构的原因描述';
COMMENT ON COLUMN "预防接种卡"."迁出日期" IS '迁出管理机构当日的公元纪年日期';
COMMENT ON COLUMN "预防接种卡"."户籍地-门牌号码" IS '户籍地中的门牌号码';
COMMENT ON COLUMN "预防接种卡"."户籍地-村(街、路、弄等)名称" IS '户籍地中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "预防接种卡"."户籍地-村(街、路、弄等)代码" IS '户籍地中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."户籍地-乡(镇、街道办事处)名称" IS '户籍地中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "预防接种卡"."户籍地-乡(镇、街道办事处)代码" IS '户籍地中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."户籍地-县(市、区)名称" IS '户籍地中的县或区名称';
COMMENT ON COLUMN "预防接种卡"."户籍地-县(市、区)代码" IS '户籍地中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."户籍地-市(地区、州)名称" IS '户籍地中的市、地区或州的名称';
COMMENT ON COLUMN "预防接种卡"."户籍地-市(地区、州)代码" IS '户籍地中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."户籍地-省(自治区、直辖市)名称" IS '户籍地中的省、自治区或直辖市名称';
COMMENT ON COLUMN "预防接种卡"."户籍地-省(自治区、直辖市)代码" IS '户籍地中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."户籍地-行政区划代码" IS '户籍地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "预防接种卡"."户籍地-详细地址" IS '户籍地址的详细描述';
COMMENT ON COLUMN "预防接种卡"."现住址-门牌号码" IS '接种者现住地址中的门牌号码';
COMMENT ON COLUMN "预防接种卡"."现住址-村(街、路、弄等)名称" IS '接种者现住地址中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "预防接种卡"."现住址-村(街、路、弄等)代码" IS '接种者现住地址中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."现住址-乡(镇、街道办事处)名称" IS '接种者现住地址中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "预防接种卡"."现地址-乡(镇、街道办事处)代码" IS '接种者现住地址中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."现住址-县(市、区)名称" IS '接种者现住地址中的县或区名称';
COMMENT ON COLUMN "预防接种卡"."现住址-县(市、区)代码" IS '接种者现住地址中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."现住址-市(地区、州)名称" IS '接种者现住地址中的市、地区或州的名称';
COMMENT ON COLUMN "预防接种卡"."现住址-市(地区、州)代码" IS '接种者现住地址中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "预防接种卡"."现住址-省(自治区、直辖市)代码" IS '接种者现住地址中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."现住址行政区划代码" IS '接种者现住地址区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "预防接种卡"."现住详细地址" IS '接种者现住地址址的详细描述';
COMMENT ON COLUMN "预防接种卡"."邮政编码" IS '接种者所在地区的邮政编码';
COMMENT ON COLUMN "预防接种卡"."工作单位电话号码" IS '工作单位的电话号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "预防接种卡"."家人电话号码" IS '家人的电话号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "预防接种卡"."本人电话号码" IS '接种者本人的电话号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "预防接种卡"."监护人与本人关系名称" IS '监护人与本人之间的关系类别的标准名称，如配偶、子女、父母等';
COMMENT ON COLUMN "预防接种卡"."监护人与本人关系代码" IS '监护人与本人之间的关系类别(如配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."监护人姓名" IS '监护人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "预防接种卡"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "预防接种卡"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "预防接种卡"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "预防接种卡"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "预防接种卡"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "预防接种卡"."姓名" IS '接种者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "预防接种卡"."城乡居民健康档案编号" IS '按照某一特定编码规则赋予个体城乡居民健康档案的编号';
COMMENT ON COLUMN "预防接种卡"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "预防接种卡"."预防接种卡编号" IS '按照某一特定编码规则赋予本人预防接种卡的顺序号';
COMMENT ON COLUMN "预防接种卡"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "预防接种卡"."接种流水号" IS '按照一定编码规则赋予评估记录的顺序号';
COMMENT ON COLUMN "预防接种卡"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "预防接种卡"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "预防接种卡"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "预防接种卡"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "预防接种卡"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "预防接种卡"."迁入日期" IS '迁入管理机构当日的公元日期';
COMMENT ON COLUMN "预防接种卡"."疫苗异常反应史" IS '疫苗接种异常反应史的详细描述';
COMMENT ON COLUMN "预防接种卡"."接种禁忌" IS '本人因患某些疾病、发生某些情况或特定的人群(丿L童、老年人、孕妇及哺乳期妇女、肝肾功能不全者等〉不适宜接种某疫苗时的详细描述';
COMMENT ON TABLE "预防接种卡" IS '预防接种信息，包括接种人基本信息、监护人信息、疫苗信息、预防接种后不良反应情况等';


CREATE TABLE IF NOT EXISTS "阴道分娩记录" (
"产瘤部位" varchar (100) DEFAULT NULL,
 "Apgar评分间隔时间代码" varchar (1) DEFAULT NULL,
 "Apgar评分值" decimal (2,
 0) DEFAULT NULL,
 "分娩结局代码" varchar (1) DEFAULT NULL,
 "新生儿异常情况代码" varchar (1) DEFAULT NULL,
 "接生人员工号" varchar (20) DEFAULT NULL,
 "接生人员姓名" varchar (50) DEFAULT NULL,
 "手术医生工号" varchar (20) DEFAULT NULL,
 "手术医生姓名" varchar (50) DEFAULT NULL,
 "儿科医生工号" varchar (20) DEFAULT NULL,
 "儿科医生姓名" varchar (50) DEFAULT NULL,
 "记录人姓名" varchar (50) DEFAULT NULL,
 "记录人工号" varchar (20) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "产后心率(次/min)" decimal (3,
 0) DEFAULT NULL,
 "宫口开全时间" timestamp DEFAULT NULL,
 "胎盘娩出情况" varchar (100) DEFAULT NULL,
 "胎膜完整情况标志" varchar (1) DEFAULT NULL,
 "绕颈身(周)" decimal (3,
 0) DEFAULT NULL,
 "脐带长度(cm)" decimal (5,
 0) DEFAULT NULL,
 "脐带异常情况描述" varchar (200) DEFAULT NULL,
 "产时用药" varchar (50) DEFAULT NULL,
 "预防措施" varchar (200) DEFAULT NULL,
 "会阴切开标志" varchar (1) DEFAULT NULL,
 "会阴切开位置" varchar (100) DEFAULT NULL,
 "会阴缝合针数" decimal (2,
 0) DEFAULT NULL,
 "会阴裂伤情况代码" varchar (1) DEFAULT NULL,
 "会阴血肿标志" varchar (1) DEFAULT NULL,
 "麻醉方法代码" varchar (20) DEFAULT NULL,
 "麻醉药物名称" varchar (250) DEFAULT NULL,
 "阴道裂伤标志" varchar (1) DEFAULT NULL,
 "阴道血肿标志" varchar (1) DEFAULT NULL,
 "阴道血肿大小" varchar (50) DEFAULT NULL,
 "阴道血肿处理" varchar (200) DEFAULT NULL,
 "宫颈裂伤标志" varchar (1) DEFAULT NULL,
 "宫颈缝合情况" varchar (100) DEFAULT NULL,
 "肛查" varchar (100) DEFAULT NULL,
 "产后用药" varchar (50) DEFAULT NULL,
 "分娩过程特殊情况" varchar (200) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "宫缩情况" varchar (200) DEFAULT NULL,
 "阴道分娩记录流水号" varchar (64) NOT NULL,
 "第1产程时长(min)" decimal (4,
 0) DEFAULT NULL,
 "子宫情况" varchar (100) DEFAULT NULL,
 "恶露状况" varchar (100) DEFAULT NULL,
 "修补手术过程" varchar (100) DEFAULT NULL,
 "存脐带血情况标志" varchar (1) DEFAULT NULL,
 "产后诊断" varchar (200) DEFAULT NULL,
 "产后观察时间" timestamp DEFAULT NULL,
 "产后检查时间(min)" decimal (3,
 0) DEFAULT NULL,
 "产后收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "产后舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "产后脉搏(次/min)" decimal (3,
 0) DEFAULT NULL,
 "胎盘娩出时间" timestamp DEFAULT NULL,
 "前羊水性状" varchar (100) DEFAULT NULL,
 "胎方位代码" varchar (2) DEFAULT NULL,
 "胎膜破裂时间" timestamp DEFAULT NULL,
 "入院诊断描述" varchar (200) DEFAULT NULL,
 "临产时间" timestamp DEFAULT NULL,
 "待产时间" timestamp DEFAULT NULL,
 "预产期" date DEFAULT NULL,
 "末次月经日期" date DEFAULT NULL,
 "产次" decimal (2,
 0) DEFAULT NULL,
 "孕次" decimal (2,
 0) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "羊水量" decimal (5,
 0) DEFAULT NULL,
 "羊水性状" varchar (100) DEFAULT NULL,
 "阴道助产方式" varchar (100) DEFAULT NULL,
 "阴道助产标志" varchar (1) DEFAULT NULL,
 "总产程时长(min)" decimal (4,
 0) DEFAULT NULL,
 "产妇姓名" varchar (50) DEFAULT NULL,
 "第3产程时长(min)" decimal (4,
 0) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "胎儿娩出时间" timestamp DEFAULT NULL,
 "第2产程时长(min)" decimal (4,
 0) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "前羊水量" decimal (5,
 0) DEFAULT NULL,
 "待产记录流水号" varchar (64) DEFAULT NULL,
 "产后出血量(mL)" decimal (5,
 0) DEFAULT NULL,
 "产后宫缩" varchar (200) DEFAULT NULL,
 "产后宫底高度(cm)" decimal (4,
 1) DEFAULT NULL,
 "产后膀胱充盈标志" varchar (1) DEFAULT NULL,
 "新生儿性别代码" varchar (2) DEFAULT NULL,
 "新生儿出生体重(g)" decimal (4,
 0) DEFAULT NULL,
 "新生儿出生身长(cm)" decimal (5,
 1) DEFAULT NULL,
 "产瘤大小" varchar (100) DEFAULT NULL,
 CONSTRAINT "阴道分娩记录"_"阴道分娩记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("阴道分娩记录流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "阴道分娩记录"."产瘤大小" IS '产瘤大小的详细描述，计量为单位';
COMMENT ON COLUMN "阴道分娩记录"."新生儿出生身长(cm)" IS '新生儿出生卧位身高的测量值,计量单位为cm';
COMMENT ON COLUMN "阴道分娩记录"."新生儿出生体重(g)" IS '新生儿出生后第1小时内第1次称得的重量，计量单位为g';
COMMENT ON COLUMN "阴道分娩记录"."新生儿性别代码" IS '新生儿生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "阴道分娩记录"."产后膀胱充盈标志" IS '标识产后膀胱是否充盈的标志';
COMMENT ON COLUMN "阴道分娩记录"."产后宫底高度(cm)" IS '产妇产后耻骨联合上缘至子宫底部距离的测量值，计量单位为 cm';
COMMENT ON COLUMN "阴道分娩记录"."产后宫缩" IS '产妇分娩后宫缩强度等情况的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."产后出血量(mL)" IS '产妇产时和产后出血量的累计值，计量单位为 mL';
COMMENT ON COLUMN "阴道分娩记录"."待产记录流水号" IS '按照某一特定编码规则赋予待产记录的顺序号，是待产记录的唯一标识';
COMMENT ON COLUMN "阴道分娩记录"."前羊水量" IS '前羊水量的描述,单位为mL';
COMMENT ON COLUMN "阴道分娩记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "阴道分娩记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "阴道分娩记录"."第2产程时长(min)" IS '产妇分娩过程中，从宫口开全到胎儿娩出的时长，计量单位为min';
COMMENT ON COLUMN "阴道分娩记录"."胎儿娩出时间" IS '胎儿娩出时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "阴道分娩记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "阴道分娩记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "阴道分娩记录"."住院次数" IS '表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "阴道分娩记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "阴道分娩记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "阴道分娩记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "阴道分娩记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "阴道分娩记录"."第3产程时长(min)" IS '产妇分娩过程中，从胎儿娩出到胎盘娩出的时长，计量单位为min';
COMMENT ON COLUMN "阴道分娩记录"."产妇姓名" IS '产妇本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "阴道分娩记录"."总产程时长(min)" IS '产妇分娩过程中，从开始出现规律官缩到胎盘娩出的时间长度，计量单位为min';
COMMENT ON COLUMN "阴道分娩记录"."阴道助产标志" IS '标识受检者是否阴道助产的标志';
COMMENT ON COLUMN "阴道分娩记录"."阴道助产方式" IS '阴道助产方式的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."羊水性状" IS '羊水性状的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."羊水量" IS '羊水量的描述，单位为mL';
COMMENT ON COLUMN "阴道分娩记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "阴道分娩记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "阴道分娩记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "阴道分娩记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "阴道分娩记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "阴道分娩记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "阴道分娩记录"."孕次" IS '妊娠次数的累计值，包括异位妊娠，计量单位为次';
COMMENT ON COLUMN "阴道分娩记录"."产次" IS '产妇分娩总次数，包括28周后的引产，双多胎分娩只计1次';
COMMENT ON COLUMN "阴道分娩记录"."末次月经日期" IS '末次月经首日的公元纪年日期的完整描述';
COMMENT ON COLUMN "阴道分娩记录"."预产期" IS '根据产妇末次月经首日推算的顶产期的公元纪年日期的完整描述';
COMMENT ON COLUMN "阴道分娩记录"."待产时间" IS '产妇进入产房时的公元纪年日期和吋间的完整描述';
COMMENT ON COLUMN "阴道分娩记录"."临产时间" IS '产妇规律宫缩开始时的公元纪年日期和吋间的完整描述';
COMMENT ON COLUMN "阴道分娩记录"."入院诊断描述" IS '人院分娩前诊断的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."胎膜破裂时间" IS '胎膜破裂时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "阴道分娩记录"."胎方位代码" IS '胎儿方位的类别在特定编码体系中的代码';
COMMENT ON COLUMN "阴道分娩记录"."前羊水性状" IS '前羊水性状的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."胎盘娩出时间" IS '胎盘娩出时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "阴道分娩记录"."产后脉搏(次/min)" IS '产后每分钟脉搏的次数测量值，计量单位为次/min';
COMMENT ON COLUMN "阴道分娩记录"."产后舒张压(mmHg)" IS '产后舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "阴道分娩记录"."产后收缩压(mmHg)" IS '产后收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "阴道分娩记录"."产后检查时间(min)" IS '产后检查时， 距离分娩结朿后的时间， 计量单位为min';
COMMENT ON COLUMN "阴道分娩记录"."产后观察时间" IS '产后观察结束时间的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "阴道分娩记录"."产后诊断" IS '对产妇产后诊断(包括孕产次数)的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."存脐带血情况标志" IS '标识是否留存脐带血的标志';
COMMENT ON COLUMN "阴道分娩记录"."修补手术过程" IS '修补手术过程情况的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."恶露状况" IS '对产妇产后恶露检查结果的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."子宫情况" IS '子宫情况的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."第1产程时长(min)" IS '产妇分娩过程中，从开始出现间歇3min~4min的规律宫缩到宫口开全的时长，计量单位为min';
COMMENT ON COLUMN "阴道分娩记录"."阴道分娩记录流水号" IS '按照某一特定编码规则赋予阴道分娩记录的顺序号';
COMMENT ON COLUMN "阴道分娩记录"."宫缩情况" IS '产妇分娩过程宫缩强度等情况的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "阴道分娩记录"."分娩过程特殊情况" IS '产妇分娩过程中特殊情况的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."产后用药" IS '医疗机构内产后所用药物名称，指药品的通用名';
COMMENT ON COLUMN "阴道分娩记录"."肛查" IS '产妇分娩后肛查情况的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."宫颈缝合情况" IS '产妇宫颈缝合情况的详细描述，如缝合针数等';
COMMENT ON COLUMN "阴道分娩记录"."宫颈裂伤标志" IS '标识宫颈是否存在裂伤的标志';
COMMENT ON COLUMN "阴道分娩记录"."阴道血肿处理" IS '阴道血肿处理情况的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."阴道血肿大小" IS '阴道血肿大小的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."阴道血肿标志" IS '标识阴道是否存在血肿的标志';
COMMENT ON COLUMN "阴道分娩记录"."阴道裂伤标志" IS '标识阴道是否存在裂伤的标志';
COMMENT ON COLUMN "阴道分娩记录"."麻醉药物名称" IS '平台中心麻醉药物名称';
COMMENT ON COLUMN "阴道分娩记录"."麻醉方法代码" IS '为患者进行手术、操作时使用的麻醉方法在特定编码体系中的代码';
COMMENT ON COLUMN "阴道分娩记录"."会阴血肿标志" IS '标识会阴是否存在血肿的标志';
COMMENT ON COLUMN "阴道分娩记录"."会阴裂伤情况代码" IS '产妇会阴裂伤的程度(如无裂伤、Ⅰ°裂伤、Ⅱ°裂伤、Ⅲ°裂伤、会阴切开等)在特定编码体系中的代码';
COMMENT ON COLUMN "阴道分娩记录"."会阴缝合针数" IS '产妇会阴缝合针数的计数值';
COMMENT ON COLUMN "阴道分娩记录"."会阴切开位置" IS '会阴切开位置的描述，如左侧、右侧等';
COMMENT ON COLUMN "阴道分娩记录"."会阴切开标志" IS '标识产妇是否行会阴切开操作的标志';
COMMENT ON COLUMN "阴道分娩记录"."预防措施" IS '对产妇进行预防措施的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."产时用药" IS '医疗机构内产时所用药物名称，指药品的通用名';
COMMENT ON COLUMN "阴道分娩记录"."脐带异常情况描述" IS '标识脐带是否存在异常情况的详细描述';
COMMENT ON COLUMN "阴道分娩记录"."脐带长度(cm)" IS '脐带的长度值，计量单位为cm';
COMMENT ON COLUMN "阴道分娩记录"."绕颈身(周)" IS '脐带绕颈身的周数，计诋单位为周';
COMMENT ON COLUMN "阴道分娩记录"."胎膜完整情况标志" IS '标识胎膜是否完盤的标志';
COMMENT ON COLUMN "阴道分娩记录"."胎盘娩出情况" IS '对胎盘娩出情况的描述,如娩出方式、胎盘重量、胎盘大小、胎盘完整情况、胎盘附着位置等';
COMMENT ON COLUMN "阴道分娩记录"."宫口开全时间" IS '产妇宫口开全时的公元纪年日期和吋间的完整描述';
COMMENT ON COLUMN "阴道分娩记录"."产后心率(次/min)" IS '产后心脏搏动频率的测量值，计量单位为次/min';
COMMENT ON COLUMN "阴道分娩记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "阴道分娩记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "阴道分娩记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "阴道分娩记录"."记录人工号" IS '记录人员在机构内特定编码体系中的编号';
COMMENT ON COLUMN "阴道分娩记录"."记录人姓名" IS '记录人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "阴道分娩记录"."儿科医生姓名" IS '儿科医师在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "阴道分娩记录"."儿科医生工号" IS '儿科医师在原始特定编码体系中的编号';
COMMENT ON COLUMN "阴道分娩记录"."手术医生姓名" IS '实施手术的主要执行人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "阴道分娩记录"."手术医生工号" IS '实施手术的主要执行人员在原始特定编码体系中的编号';
COMMENT ON COLUMN "阴道分娩记录"."接生人员姓名" IS '接生人员签署的在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "阴道分娩记录"."接生人员工号" IS '接生人员的工号';
COMMENT ON COLUMN "阴道分娩记录"."新生儿异常情况代码" IS '新生儿异常情况类别(如无、早期新生儿死亡、畸形、早产等)在特定编码体系中的代码';
COMMENT ON COLUMN "阴道分娩记录"."分娩结局代码" IS '孕产妇妊娠分娩最终结局(如活产、死胎、死产等)在特定编码体系中的代码';
COMMENT ON COLUMN "阴道分娩记录"."Apgar评分值" IS '对新生儿娩出后呼吸、心率、皮肤颜色、肌张力及对刺激的反应等5 项指标的评分结果值，计量单位为分';
COMMENT ON COLUMN "阴道分娩记录"."Apgar评分间隔时间代码" IS 'Apgar评分间隔时间(如1分钟、5分钟、10分钟)在特定编码体系中的代码';
COMMENT ON COLUMN "阴道分娩记录"."产瘤部位" IS '产瘤部位的详细描述';
COMMENT ON TABLE "阴道分娩记录" IS '阴道分娩产时记录，包括产程、助产方式、阴道胎盘胎盘情况，新生儿评分';


CREATE TABLE IF NOT EXISTS "门急诊挂号情况表" (
"当前已挂号人数" decimal (10,
 0) DEFAULT NULL,
 "当前已接诊人数" decimal (10,
 0) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "采集日期" date NOT NULL,
 "采集时间" timestamp NOT NULL,
 "科室代码" varchar (20) NOT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "医生工号" varchar (20) NOT NULL,
 "医生姓名" varchar (50) DEFAULT NULL,
 "挂号类别代码" varchar (10) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "本日预定放号数" decimal (10,
 0) DEFAULT NULL,
 CONSTRAINT "门急诊挂号情况表"_"医疗机构代码"_"采集日期"_"采集时间"_"科室代码"_"医生工号"_"挂号类别代码"_PK PRIMARY KEY ("医疗机构代码",
 "采集日期",
 "采集时间",
 "科室代码",
 "医生工号",
 "挂号类别代码")
);
COMMENT ON COLUMN "门急诊挂号情况表"."本日预定放号数" IS '对指定医师若有预定放号数量时填写';
COMMENT ON COLUMN "门急诊挂号情况表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "门急诊挂号情况表"."挂号类别代码" IS '挂号类别(如普通门诊、急诊、特需门诊、免费号等)在特定编码体系中的代码';
COMMENT ON COLUMN "门急诊挂号情况表"."医生姓名" IS '挂号医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "门急诊挂号情况表"."医生工号" IS '挂号医师在特定编码体系中的工号';
COMMENT ON COLUMN "门急诊挂号情况表"."科室名称" IS '所在科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等。若医院以科室进行划分，则填写科室代码；若以病区划分的则填写病区代码。通常是按病区划分的';
COMMENT ON COLUMN "门急诊挂号情况表"."科室代码" IS '按照机构内编码规则赋予所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识。若医院以科室进行划分，则填写科室代码；若以病区划分的则填写病区代码。通常是按病区划分的';
COMMENT ON COLUMN "门急诊挂号情况表"."采集时间" IS '数据采集当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门急诊挂号情况表"."采集日期" IS '数据采集当日的公元纪年日期';
COMMENT ON COLUMN "门急诊挂号情况表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "门急诊挂号情况表"."医疗机构代码" IS '医疗机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "门急诊挂号情况表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门急诊挂号情况表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门急诊挂号情况表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "门急诊挂号情况表"."当前已接诊人数" IS '指本日开始截止到采集当时的接诊人数';
COMMENT ON COLUMN "门急诊挂号情况表"."当前已挂号人数" IS '指本日开始截止到采集当时的挂号人数';
COMMENT ON TABLE "门急诊挂号情况表" IS '按科室统计医院日工作量和病床数';


CREATE TABLE IF NOT EXISTS "门急诊工作量分科统计表(
日报)" ("门诊人数" decimal (15,
 0) DEFAULT NULL,
 "平台科室名称" varchar (100) DEFAULT NULL,
 "平台科室代码" varchar (20) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "急诊新农合人数" decimal (15,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "科室代码" varchar (20) NOT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "业务日期" date NOT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "急诊接受中医治疗人次" decimal (15,
 0) DEFAULT NULL,
 "门诊接受中医治疗人次" decimal (15,
 0) DEFAULT NULL,
 "治未病门诊人次" decimal (15,
 0) DEFAULT NULL,
 "门诊中医预防保健人次" decimal (15,
 0) DEFAULT NULL,
 "急诊使用注射药物人次" decimal (15,
 0) DEFAULT NULL,
 "门诊使用注射药物人次" decimal (15,
 0) DEFAULT NULL,
 "急诊自费人次" decimal (15,
 0) DEFAULT NULL,
 "门诊自费人次" decimal (15,
 0) DEFAULT NULL,
 "急诊新农合人次" decimal (15,
 0) DEFAULT NULL,
 "门诊新农合人次" decimal (15,
 0) DEFAULT NULL,
 "急诊医保人次" decimal (15,
 0) DEFAULT NULL,
 "门诊医保人次" decimal (15,
 0) DEFAULT NULL,
 "门急诊预约诊疗次" decimal (15,
 0) DEFAULT NULL,
 "急诊延迟就诊人次" decimal (15,
 0) DEFAULT NULL,
 "急诊空挂号人次" decimal (15,
 0) DEFAULT NULL,
 "急诊退号人次" decimal (15,
 0) DEFAULT NULL,
 "急诊挂号人次" decimal (15,
 0) DEFAULT NULL,
 "发热急诊人次" decimal (15,
 0) DEFAULT NULL,
 "中暑急诊人次" decimal (15,
 0) DEFAULT NULL,
 "肠道急诊人次" decimal (15,
 0) DEFAULT NULL,
 "儿科急诊人次" decimal (15,
 0) DEFAULT NULL,
 "急诊人次" decimal (15,
 0) DEFAULT NULL,
 "门诊延迟就诊人次" decimal (15,
 0) DEFAULT NULL,
 "门诊空挂号人次" decimal (15,
 0) DEFAULT NULL,
 "门诊退号人次" decimal (15,
 0) DEFAULT NULL,
 "门诊挂号人次" decimal (15,
 0) DEFAULT NULL,
 "夜门诊人次" decimal (15,
 0) DEFAULT NULL,
 "特需门诊人次" decimal (15,
 0) DEFAULT NULL,
 "专家门诊人次" decimal (15,
 0) DEFAULT NULL,
 "门诊人次" decimal (15,
 0) DEFAULT NULL,
 "总诊疗人次" decimal (15,
 0) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "门诊新农合人数" decimal (15,
 0) DEFAULT NULL,
 "急诊医保人数" decimal (15,
 0) DEFAULT NULL,
 "门诊医保人数" decimal (15,
 0) DEFAULT NULL,
 "急诊人数" decimal (15,
 0) DEFAULT NULL,
 CONSTRAINT "门急诊工作量分科统计表(日报)"_"科室代码"_"业务日期"_"医疗机构代码"_PK PRIMARY KEY ("科室代码",
 "业务日期",
 "医疗机构代码")
);
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊人数" IS '统计日期内的急诊人数';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊医保人数" IS '统计日期内的门诊医保人数';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊医保人数" IS '统计日期内的急诊医保人数';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊新农合人数" IS '统计日期内的门诊新农合人数';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."医疗机构代码" IS '填报医疗机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."总诊疗人次" IS '统计日期内的总诊疗人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊人次" IS '统计日期内的门诊人次。门诊人次=门诊挂号人次数-门诊退号人次数-门诊空挂号人次数+门诊延迟就诊人次数';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."专家门诊人次" IS '统计日期内的专家门诊人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."特需门诊人次" IS '统计日期内的特需门诊人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."夜门诊人次" IS '统计日期内的夜门诊人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊挂号人次" IS '统计日期内的门诊挂号人次。挂号方式包括窗口挂号、自助挂号、网上挂号等方式';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊退号人次" IS '统计日期内的门诊退号人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊空挂号人次" IS '统计日期内的门诊空挂号人次。空挂号人次数是指只有挂号而没有就诊记录的人次数';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊延迟就诊人次" IS '统计日期内的门诊延迟就诊人次。延迟就诊人次数是指挂号当日没有就诊而在挂号有效期内就诊的人次数';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊人次" IS '统计日期内的急诊人次。急诊人次=急诊挂号人次数-急诊退号人次数-急诊空挂号人次数+急诊延迟就诊人次数';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."儿科急诊人次" IS '统计日期内的儿科急诊人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."肠道急诊人次" IS '统计日期内的肠道急诊人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."中暑急诊人次" IS '统计日期内的中暑急诊人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."发热急诊人次" IS '统计日期内的发热急诊人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊挂号人次" IS '统计日期内的急诊挂号人次。挂号方式包括窗口挂号、自助挂号、网上挂号等方式';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊退号人次" IS '统计日期内的急诊退号人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊空挂号人次" IS '统计日期内的急诊空挂号人次。空挂号人次数是指只有挂号而没有就诊记录的人次数';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊延迟就诊人次" IS '统计日期内的急诊延迟就诊人次。延迟就诊人次数是指挂号当日没有就诊而在挂号有效期内就诊的人次数';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门急诊预约诊疗次" IS '统计日期内的门急诊预约诊疗次数';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊医保人次" IS '统计日期内的门诊医保人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊医保人次" IS '统计日期内的急诊医保人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊新农合人次" IS '统计日期内的门诊新农合人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊新农合人次" IS '统计日期内的急诊新农合人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊自费人次" IS '统计日期内的门诊自费人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊自费人次" IS '统计日期内的急诊自费人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊使用注射药物人次" IS '统计日期内的门诊使用注射药物人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊使用注射药物人次" IS '统计日期内的急诊使用注射药物人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊中医预防保健人次" IS '统计日期内的门诊中医预防保健人次。门诊中医预防保健人次=治未病门诊人次+中医科门诊人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."治未病门诊人次" IS '统计日期内的治未病门诊人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊接受中医治疗人次" IS '统计日期内的门诊接受中医治疗人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊接受中医治疗人次" IS '统计日期内的急诊接受中医治疗人次';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."业务日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."科室名称" IS '科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."科室代码" IS '按照机构内编码规则赋予科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."急诊新农合人数" IS '统计日期内的急诊新农合人数';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."平台科室代码" IS '按照平台特定编码规则赋予科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."平台科室名称" IS '科室在平台特定编码体系中的名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "门急诊工作量分科统计表(日报)"."门诊人数" IS '统计日期内的门诊人数';
COMMENT ON TABLE "门急诊工作量分科统计表(日报)" IS '按科室统计日门急诊工作量，包括门急诊人数、人次、挂号人次等';


CREATE TABLE IF NOT EXISTS "造血干细胞移植术记录" (
"供者家庭关系代码" varchar (10) DEFAULT NULL,
 "TBI_照射标志" varchar (1) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "造血干细胞移植术序号" varchar (64) DEFAULT NULL,
 "供者与患者关系" varchar (10) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "填表人姓名" varchar (50) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "填表人工号" varchar (20) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "主刀医师姓名" varchar (50) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "主刀医师工号" varchar (20) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "复发表现" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "复发时间" timestamp DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "复发标志" varchar (1) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "综合程度" varchar (10) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "发生aGVHD标志" varchar (1) DEFAULT NULL,
 "手术记录流水号" varchar (64) NOT NULL,
 "血小板植入天数" decimal (10,
 0) DEFAULT NULL,
 "手术申请单号" varchar (32) DEFAULT NULL,
 "中性粒细胞植入天数" decimal (10,
 0) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "植入失败标志" varchar (1) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院药费" decimal (10,
 2) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "住院费" decimal (18,
 3) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "住院天数" decimal (5,
 0) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "CD34+细胞数2" decimal (20,
 0) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "CD34+细胞数1" decimal (20,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "输入单个核细胞数-第二份" decimal (20,
 0) DEFAULT NULL,
 "移植后天数" decimal (20,
 0) DEFAULT NULL,
 "输入单个核细胞数-第一份" decimal (20,
 0) DEFAULT NULL,
 "疾病诊断名称" varchar (512) DEFAULT NULL,
 "脐带血相合情况代码" varchar (10) DEFAULT NULL,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "脐带血配型代码" varchar (10) DEFAULT NULL,
 "恶性疾病类别" varchar (10) DEFAULT NULL,
 "外周血相合情况代码" varchar (10) DEFAULT NULL,
 "非恶性疾病类别" varchar (10) DEFAULT NULL,
 "外周血配型代码" varchar (10) DEFAULT NULL,
 "受体移植前疾病状态" decimal (1,
 0) DEFAULT NULL,
 "骨髓相合情况代码" varchar (10) DEFAULT NULL,
 "第一份干细胞回输时间" timestamp DEFAULT NULL,
 "骨髓配型代码" varchar (10) DEFAULT NULL,
 "第二份干细胞回输时间" timestamp DEFAULT NULL,
 "移植方式" decimal (1,
 0) DEFAULT NULL,
 "第三份干细胞回输时间" timestamp DEFAULT NULL,
 "受者家庭关系代码" varchar (10) DEFAULT NULL,
 "预处理方案" varchar (10) DEFAULT NULL,
 CONSTRAINT "造血干细胞移植术记录"_"医疗机构代码"_"手术记录流水号"_PK PRIMARY KEY ("医疗机构代码",
 "手术记录流水号")
);
COMMENT ON COLUMN "造血干细胞移植术记录"."预处理方案" IS '预处理方案的在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."受者家庭关系代码" IS '受者分类在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."第三份干细胞回输时间" IS '第三份干细胞回输时间的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "造血干细胞移植术记录"."移植方式" IS '移植方式在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."第二份干细胞回输时间" IS '第二份干细胞回输时间的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "造血干细胞移植术记录"."骨髓配型代码" IS '骨髓在配型编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."第一份干细胞回输时间" IS '第一份干细胞回输时间的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "造血干细胞移植术记录"."骨髓相合情况代码" IS '骨髓相合在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."受体移植前疾病状态" IS '受体移植前疾病状态的特定代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."外周血配型代码" IS '外周血在配型编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."非恶性疾病类别" IS '非恶性疾病在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."外周血相合情况代码" IS '外周血相合在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."恶性疾病类别" IS '恶性疾病分类在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."脐带血配型代码" IS '脐带血在配型编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."疾病诊断代码" IS '疾病诊断在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."脐带血相合情况代码" IS '在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."疾病诊断名称" IS '疾病诊断在特定编码体系中的名称';
COMMENT ON COLUMN "造血干细胞移植术记录"."输入单个核细胞数-第一份" IS '输入单个核细胞数-第一份的数量描述';
COMMENT ON COLUMN "造血干细胞移植术记录"."移植后天数" IS '移植后的天数';
COMMENT ON COLUMN "造血干细胞移植术记录"."输入单个核细胞数-第二份" IS '输入单个核细胞数-第二份的数量描述';
COMMENT ON COLUMN "造血干细胞移植术记录"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "造血干细胞移植术记录"."CD34+细胞数1" IS 'CD34+细胞数1的数量描述';
COMMENT ON COLUMN "造血干细胞移植术记录"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "造血干细胞移植术记录"."CD34+细胞数2" IS 'CD34+细胞数2的数量描述';
COMMENT ON COLUMN "造血干细胞移植术记录"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."住院天数" IS '患者实际的住院天数，入院日与出院日只计算1天';
COMMENT ON COLUMN "造血干细胞移植术记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "造血干细胞移植术记录"."住院费" IS '住院总费用，计量单位为人民币元';
COMMENT ON COLUMN "造血干细胞移植术记录"."住院次数" IS '此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "造血干细胞移植术记录"."住院药费" IS '住院费用中药品类的费用总额，计量单位为人民币元';
COMMENT ON COLUMN "造血干细胞移植术记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "造血干细胞移植术记录"."植入失败标志" IS '植入是否失败标志';
COMMENT ON COLUMN "造血干细胞移植术记录"."病案号" IS '按照某一特定编码规则赋予患者在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "造血干细胞移植术记录"."中性粒细胞植入天数" IS '中性粒细胞植入天数的数值描述';
COMMENT ON COLUMN "造血干细胞移植术记录"."手术申请单号" IS '按照某一特定编码规则赋予手术申请单号的唯一标识';
COMMENT ON COLUMN "造血干细胞移植术记录"."血小板植入天数" IS '血小板植入天数的数值描述';
COMMENT ON COLUMN "造血干细胞移植术记录"."手术记录流水号" IS '按照某一特定编码规则赋予手术记录的唯一标识';
COMMENT ON COLUMN "造血干细胞移植术记录"."发生aGVHD标志" IS '是否发生aGVHD标志';
COMMENT ON COLUMN "造血干细胞移植术记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "造血干细胞移植术记录"."综合程度" IS '综合程度在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."复发标志" IS '是否复发标志';
COMMENT ON COLUMN "造血干细胞移植术记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "造血干细胞移植术记录"."复发时间" IS '复发时间的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "造血干细胞移植术记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "造血干细胞移植术记录"."复发表现" IS '复发表现在特定编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "造血干细胞移植术记录"."主刀医师工号" IS '主刀医师的工号';
COMMENT ON COLUMN "造血干细胞移植术记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "造血干细胞移植术记录"."主刀医师姓名" IS '主刀医师专业技术职务资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "造血干细胞移植术记录"."就诊科室名称" IS '就诊科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在机构内编码体系中的名称';
COMMENT ON COLUMN "造血干细胞移植术记录"."填表人工号" IS '填表人在原始特定编码体系中的编号';
COMMENT ON COLUMN "造血干细胞移植术记录"."就诊科室代码" IS '就诊科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在机构内编码体系中的代码';
COMMENT ON COLUMN "造血干细胞移植术记录"."填表人姓名" IS '填表人在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "造血干细胞移植术记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "造血干细胞移植术记录"."供者与患者关系" IS '供者与患者关系标志';
COMMENT ON COLUMN "造血干细胞移植术记录"."造血干细胞移植术序号" IS '按照某一特定编码规则赋予造血干细胞移植术序号的唯一标识';
COMMENT ON COLUMN "造血干细胞移植术记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "造血干细胞移植术记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "造血干细胞移植术记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "造血干细胞移植术记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "造血干细胞移植术记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "造血干细胞移植术记录"."TBI_照射标志" IS '有无_TBI_照射标志';
COMMENT ON COLUMN "造血干细胞移植术记录"."供者家庭关系代码" IS '供者分类在特定编码体系中的代码';
COMMENT ON TABLE "造血干细胞移植术记录" IS '造血干细胞移植术记录，包括疾病名称、干细胞回输时间、供着信息、移植方式、单个核细胞数等';


CREATE TABLE IF NOT EXISTS "输血申请表" (
"乙肝表面抗原结果" varchar (1) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "记录时间" timestamp DEFAULT NULL,
 "申请单审核时间" timestamp DEFAULT NULL,
 "审核状态" varchar (100) DEFAULT NULL,
 "审核医生姓名" varchar (50) DEFAULT NULL,
 "审核医生工号" varchar (20) DEFAULT NULL,
 "申请时间" timestamp DEFAULT NULL,
 "申请医生姓名" varchar (50) DEFAULT NULL,
 "申请医生工号" varchar (20) DEFAULT NULL,
 "活化部分凝血活酶时间" varchar (50) DEFAULT NULL,
 "凝血酶原时间" varchar (50) DEFAULT NULL,
 "纤维蛋白原检测值" varchar (50) DEFAULT NULL,
 "血小板计数值" varchar (50) DEFAULT NULL,
 "白细胞计数值(G/L)" varchar (50) DEFAULT NULL,
 "红细胞计数值" varchar (50) DEFAULT NULL,
 "红细胞比容" varchar (50) DEFAULT NULL,
 "血红蛋白值(g/L)" varchar (3) DEFAULT NULL,
 "梅毒检测结果" varchar (1) DEFAULT NULL,
 "HIV1/2抗体检测结果" varchar (1) DEFAULT NULL,
 "丙型肝炎抗体结果" varchar (1) DEFAULT NULL,
 "谷丙转氨酶值" varchar (50) DEFAULT NULL,
 "不规则抗体检测结果" varchar (1) DEFAULT NULL,
 "ABO血型名称" varchar (50) DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "RH血型名称" varchar (50) DEFAULT NULL,
 "RH血型代码" varchar (1) DEFAULT NULL,
 "血样登记人姓名" varchar (50) DEFAULT NULL,
 "血样登记人工号" varchar (20) DEFAULT NULL,
 "手术用血标志" varchar (1) DEFAULT NULL,
 "诊断名称" varchar (100) DEFAULT NULL,
 "诊断代码" varchar (36) DEFAULT NULL,
 "输血史" text,
 "输血目的描述" varchar (50) DEFAULT NULL,
 "输血目的代码" varchar (1) DEFAULT NULL,
 "输血性质" varchar (15) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄" decimal (3,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "身份证件号码" varchar (32) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "就诊流水号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "申请流水号" varchar (36) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 CONSTRAINT "输血申请表"_"申请流水号"_"医疗机构代码"_PK PRIMARY KEY ("申请流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "输血申请表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "输血申请表"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "输血申请表"."申请流水号" IS '按照某一特定编码规则赋予输血申请单的唯一标识';
COMMENT ON COLUMN "输血申请表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "输血申请表"."就诊流水号" IS '按照某一特定编码规则赋予特定业务事件的唯一标识，如门急诊就诊流水号、住院就诊流水号。对门诊类型，一般可采用挂号时HIS产生的就诊流水号；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "输血申请表"."就诊事件类型代码" IS '就诊事件类型如门诊、住院等在特定编码体系中的代码';
COMMENT ON COLUMN "输血申请表"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "输血申请表"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血申请表"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "输血申请表"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "输血申请表"."身份证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "输血申请表"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "输血申请表"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "输血申请表"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "输血申请表"."年龄" IS '个体从出生当日公元纪年日起到计算当日止生存的时间长度，按计量单位计算';
COMMENT ON COLUMN "输血申请表"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "输血申请表"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "输血申请表"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "输血申请表"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "输血申请表"."科室代码" IS '出院科室的机构内名称';
COMMENT ON COLUMN "输血申请表"."科室名称" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "输血申请表"."输血性质" IS '输血性质类别(备血、常规、紧急等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血申请表"."输血目的代码" IS '输血目的(如治疗、抢救用血、手术用血等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血申请表"."输血目的描述" IS '输血目的(如治疗、抢救用血、手术用血等)在特定编码体系中的名称';
COMMENT ON COLUMN "输血申请表"."输血史" IS '对患者既往输血史的详细描述';
COMMENT ON COLUMN "输血申请表"."诊断代码" IS '西医疾病在特定编码体系中对应的代码';
COMMENT ON COLUMN "输血申请表"."诊断名称" IS '西医疾病在特定编码体系中对应的名称';
COMMENT ON COLUMN "输血申请表"."手术用血标志" IS '标志患者输血的原因是否为手术用血的标志';
COMMENT ON COLUMN "输血申请表"."血样登记人工号" IS '血样登记人在在原始特定编码体系中的编号';
COMMENT ON COLUMN "输血申请表"."血样登记人姓名" IS '血样登记人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "输血申请表"."RH血型代码" IS '为患者实际输入的Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "输血申请表"."RH血型名称" IS '为患者实际输入的Rh血型的类别在特定编码体系中的名称';
COMMENT ON COLUMN "输血申请表"."ABO血型代码" IS '为患者实际输入的ABO血型类别在特定编码体系中的代码';
COMMENT ON COLUMN "输血申请表"."ABO血型名称" IS '为患者实际输入的ABO血型类别在特定编码体系中的名称';
COMMENT ON COLUMN "输血申请表"."不规则抗体检测结果" IS '不规则抗体定性检测结果分类，应填阴性/阳性';
COMMENT ON COLUMN "输血申请表"."谷丙转氨酶值" IS '受检者单位容积血清中谷丙转氨酶的检测数值';
COMMENT ON COLUMN "输血申请表"."丙型肝炎抗体结果" IS '丙型肝炎病毒抗体定性检测结果分类，应填阴性/阳性';
COMMENT ON COLUMN "输血申请表"."HIV1/2抗体检测结果" IS 'HIV病毒抗体定性检测结果分类，应填阴性/阳性';
COMMENT ON COLUMN "输血申请表"."梅毒检测结果" IS '梅毒抗体定性检测结果分类，应填阴性/阳性';
COMMENT ON COLUMN "输血申请表"."血红蛋白值(g/L)" IS '受检者单位容积血液中血红蛋白的检测数值';
COMMENT ON COLUMN "输血申请表"."红细胞比容" IS '受检者红细胞比容的检测数值';
COMMENT ON COLUMN "输血申请表"."红细胞计数值" IS '受检者单位容积血液内红细胞的数量值';
COMMENT ON COLUMN "输血申请表"."白细胞计数值(G/L)" IS '受检者单位容积血液中白细胞数量值，计量单位为10^9/L';
COMMENT ON COLUMN "输血申请表"."血小板计数值" IS '受检者单位容积血液内血小板的数量值，计量单位为10^9/L';
COMMENT ON COLUMN "输血申请表"."纤维蛋白原检测值" IS '受检者纤维蛋白原的检测数值';
COMMENT ON COLUMN "输血申请表"."凝血酶原时间" IS '受检者凝血酶原时间的检测数值';
COMMENT ON COLUMN "输血申请表"."活化部分凝血活酶时间" IS '受检者活化部分凝血活酶时间的检测数值';
COMMENT ON COLUMN "输血申请表"."申请医生工号" IS '申请医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "输血申请表"."申请医生姓名" IS '申请医师在在原始特定编码体系中的编号';
COMMENT ON COLUMN "输血申请表"."申请时间" IS '申请检查当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血申请表"."审核医生工号" IS '审核医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "输血申请表"."审核医生姓名" IS '审核医师在原始特定编码体系中的编号。指上级医生审核（或科主任），若无，请填写输血科审核医生';
COMMENT ON COLUMN "输血申请表"."审核状态" IS '审核状态的描述';
COMMENT ON COLUMN "输血申请表"."申请单审核时间" IS '审核完成当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血申请表"."记录时间" IS '完成记录时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血申请表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "输血申请表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血申请表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血申请表"."乙肝表面抗原结果" IS '乙型肝炎病毒表面抗原定性检测结果分类，应填阴性/阳性';
COMMENT ON TABLE "输血申请表" IS '输血申请记录，包括输血目的、输血史、诊断相关信息';


CREATE TABLE IF NOT EXISTS "输血治疗同意书" (
"性别名称" varchar (20) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "输血史标志" varchar (1) DEFAULT NULL,
 "输血方式" varchar (50) DEFAULT NULL,
 "输血指征" varchar (500) DEFAULT NULL,
 "输血品种代码" varchar (10) DEFAULT NULL,
 "输血品种名称" varchar (50) DEFAULT NULL,
 "输血风险及可能发生的不良后果" text,
 "输血前有关检查项目及结果" varchar (200) DEFAULT NULL,
 "拟定输血时间" timestamp DEFAULT NULL,
 "医疗机构意见" text,
 "患者/法定代理人意见" text,
 "患者/法定代理人姓名" varchar (50) DEFAULT NULL,
 "法定代理人与患者的关系代码" varchar (1) DEFAULT NULL,
 "法定代理人与患者的关系名称" varchar (100) DEFAULT NULL,
 "患者/法定代理人签名时间" timestamp DEFAULT NULL,
 "医师工号" varchar (20) DEFAULT NULL,
 "医师姓名" varchar (50) DEFAULT NULL,
 "医师签名时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "输血治疗同意书流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "门诊就诊流水号" varchar (32) DEFAULT NULL,
 "知情同意书编号" varchar (20) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "疾病诊断名称" varchar (512) DEFAULT NULL,
 "就诊次数" decimal (5,
 0) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 CONSTRAINT "输血治疗同意书"_"医疗机构代码"_"输血治疗同意书流水号"_PK PRIMARY KEY ("医疗机构代码",
 "输血治疗同意书流水号")
);
COMMENT ON COLUMN "输血治疗同意书"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "输血治疗同意书"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "输血治疗同意书"."就诊次数" IS '对于门(急)诊患者，此处表示患者门(急)诊的累计次数；对于住院患者，此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "输血治疗同意书"."疾病诊断名称" IS '疾病诊断在特定编码体系中的名称';
COMMENT ON COLUMN "输血治疗同意书"."疾病诊断代码" IS '疾病诊断在特定编码体系中的代码';
COMMENT ON COLUMN "输血治疗同意书"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "输血治疗同意书"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "输血治疗同意书"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "输血治疗同意书"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "输血治疗同意书"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "输血治疗同意书"."知情同意书编号" IS '按照某一特定编码规则赋予知情同意书的唯一标识';
COMMENT ON COLUMN "输血治疗同意书"."门诊就诊流水号" IS '为门诊就诊时，填写门诊就诊流水号，住院就诊流水号填“-”';
COMMENT ON COLUMN "输血治疗同意书"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "输血治疗同意书"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
COMMENT ON COLUMN "输血治疗同意书"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "输血治疗同意书"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血治疗同意书"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "输血治疗同意书"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "输血治疗同意书"."输血治疗同意书流水号" IS '按照某一特定编码规则赋予输血治疗同意书的唯一标识';
COMMENT ON COLUMN "输血治疗同意书"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "输血治疗同意书"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "输血治疗同意书"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血治疗同意书"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血治疗同意书"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "输血治疗同意书"."医师签名时间" IS '医师进行电子签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血治疗同意书"."医师姓名" IS '医师签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "输血治疗同意书"."医师工号" IS '医师的工号';
COMMENT ON COLUMN "输血治疗同意书"."患者/法定代理人签名时间" IS '患者或法定代理人签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血治疗同意书"."法定代理人与患者的关系名称" IS '本人与特定对象的关系类别(如户主、配偶、子女、父母等)名称';
COMMENT ON COLUMN "输血治疗同意书"."法定代理人与患者的关系代码" IS '本人与特定对象的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血治疗同意书"."患者/法定代理人姓名" IS '患者／法定代理人签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "输血治疗同意书"."患者/法定代理人意见" IS '患者／法定代理人对知情同意书中告知内容的意见描述';
COMMENT ON COLUMN "输血治疗同意书"."医疗机构意见" IS '在此诊疗过程中，医疗机构对患者应尽责任的陈述以及可能面临的风险或意外情况所采取的应对措施的详细描述';
COMMENT ON COLUMN "输血治疗同意书"."拟定输血时间" IS '拟对患者开始进行输血时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血治疗同意书"."输血前有关检查项目及结果" IS '输血前与输血相关的检查项目及检查结果描述，如HBsAg、Anti_HBs、梅毒等';
COMMENT ON COLUMN "输血治疗同意书"."输血风险及可能发生的不良后果" IS '输血风险及可能发生的不良后果详细描述';
COMMENT ON COLUMN "输血治疗同意书"."输血品种名称" IS '输入全血或血液成分类别在特定编码体系中的名称，如红细胞、全血、血小板、血浆等';
COMMENT ON COLUMN "输血治疗同意书"."输血品种代码" IS '输入全血或血液成分类别(如红细胞、全血、血小板、血浆等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血治疗同意书"."输血指征" IS '受血者接受输血治疗的指征描述';
COMMENT ON COLUMN "输血治疗同意书"."输血方式" IS '输血同意书中本次输血方式的详细描述，如自体输血、异体输血';
COMMENT ON COLUMN "输血治疗同意书"."输血史标志" IS '标识患者是否既往有输血经历';
COMMENT ON COLUMN "输血治疗同意书"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "输血治疗同意书"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "输血治疗同意书"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "输血治疗同意书"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "输血治疗同意书"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON TABLE "输血治疗同意书" IS '输血方式，品种、日期以及各种检查结果的说明';


CREATE TABLE IF NOT EXISTS "转诊记录_用药记录" (
"药物使用途径代码" varchar (32) DEFAULT NULL,
 "药物使用途径名称" varchar (50) DEFAULT NULL,
 "中药类别代码" varchar (1) DEFAULT NULL,
 "中药类别名称" varchar (20) DEFAULT NULL,
 "中药饮片剂数(剂)" decimal (2,
 0) DEFAULT NULL,
 "中药饮片煎煮法" varchar (500) DEFAULT NULL,
 "中药用药方法" varchar (100) DEFAULT NULL,
 "处方备注信息" varchar (200) DEFAULT NULL,
 "用药天数" decimal (3,
 0) DEFAULT NULL,
 "用药停止时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "用药记录流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "转诊记录流水号" varchar (64) DEFAULT NULL,
 "药品类别代码" varchar (2) DEFAULT NULL,
 "药品通用名" varchar (60) DEFAULT NULL,
 "药品代码" varchar (50) DEFAULT NULL,
 "药品名称" varchar (64) DEFAULT NULL,
 "药物规格" varchar (200) DEFAULT NULL,
 "药物剂型代码" varchar (3) DEFAULT NULL,
 "药物剂型名称" varchar (128) DEFAULT NULL,
 "药物使用次剂量" decimal (4,
 2) DEFAULT NULL,
 "药物使用剂量单位" varchar (20) DEFAULT NULL,
 "药物使用总剂量" decimal (11,
 2) DEFAULT NULL,
 "药物使用频率代码" varchar (2) DEFAULT NULL,
 "药物使用频率名称" varchar (20) DEFAULT NULL,
 CONSTRAINT "转诊记录_用药记录"_"用药记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("用药记录流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "转诊记录_用药记录"."药物使用频率名称" IS '单位时间内药物使用频次类别的标准名称，如每天两次、每周两次、睡前一次等';
COMMENT ON COLUMN "转诊记录_用药记录"."药物使用频率代码" IS '单位时间内药物使用的频次类别(如每天两次、每周两次、睡前一次等)在特定编码体系中的代码';
COMMENT ON COLUMN "转诊记录_用药记录"."药物使用总剂量" IS '在一定时间段内使用药物的总量。根据单次剂量、频次计算得到的总量';
COMMENT ON COLUMN "转诊记录_用药记录"."药物使用剂量单位" IS '药物使用剂量单位的标准名称，如：mg，ml等';
COMMENT ON COLUMN "转诊记录_用药记录"."药物使用次剂量" IS '单次使用药物的剂量，按剂量单位计';
COMMENT ON COLUMN "转诊记录_用药记录"."药物剂型名称" IS '药物剂型在特定编码体系中的名称，如颗粒剂、片剂、丸剂、胶囊剂等';
COMMENT ON COLUMN "转诊记录_用药记录"."药物剂型代码" IS '药物剂型类别(如颗粒剂、片剂、丸剂、胶囊剂等)在特定编码体系中的代码';
COMMENT ON COLUMN "转诊记录_用药记录"."药物规格" IS '药品规格的描述，如0.25g、5mg×28片/盒';
COMMENT ON COLUMN "转诊记录_用药记录"."药品名称" IS '平台中心药品名称';
COMMENT ON COLUMN "转诊记录_用药记录"."药品代码" IS '平台中心药品代码';
COMMENT ON COLUMN "转诊记录_用药记录"."药品通用名" IS '药品通用名指中国药品通用名称。是同一种成分或相同配方组成的药品在中国境内的通用名称，具有强制性和约束性。';
COMMENT ON COLUMN "转诊记录_用药记录"."药品类别代码" IS '药品类别(中药、西药、中草药等)在特定编码体系中的代码';
COMMENT ON COLUMN "转诊记录_用药记录"."转诊记录流水号" IS '按照某一特性编码规则赋予转诊(转院)记录的唯一标识';
COMMENT ON COLUMN "转诊记录_用药记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "转诊记录_用药记录"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "转诊记录_用药记录"."用药记录流水号" IS '按照一定编码规则赋予用药记录的唯一标识';
COMMENT ON COLUMN "转诊记录_用药记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "转诊记录_用药记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "转诊记录_用药记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "转诊记录_用药记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "转诊记录_用药记录"."用药停止时间" IS '结束使用药物时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "转诊记录_用药记录"."用药天数" IS '持续用药的合计天数，计量单位为d';
COMMENT ON COLUMN "转诊记录_用药记录"."处方备注信息" IS '对下达处方的补充说明和注意事项提示';
COMMENT ON COLUMN "转诊记录_用药记录"."中药用药方法" IS '中药的用药方法的描述，如bid 煎服，先煎、后下等';
COMMENT ON COLUMN "转诊记录_用药记录"."中药饮片煎煮法" IS '中药饮片煎煮方法描述，如水煎400mL';
COMMENT ON COLUMN "转诊记录_用药记录"."中药饮片剂数(剂)" IS '本次就诊给患者所开中药饮片的剂数，计量单位为剂';
COMMENT ON COLUMN "转诊记录_用药记录"."中药类别名称" IS '中药使用类别的标准名称，如未使用、中成药、中草药、其他中药等';
COMMENT ON COLUMN "转诊记录_用药记录"."中药类别代码" IS '中药使用类别(如未使用、中成药、中草药、其他中药)在特定编码体系中的代码';
COMMENT ON COLUMN "转诊记录_用药记录"."药物使用途径名称" IS '药物的给药途径在特定编码体系中的名称，如口服、静滴、喷喉等';
COMMENT ON COLUMN "转诊记录_用药记录"."药物使用途径代码" IS '药物使用途径(如口服、静滴、喷喉等)在特定编码体系中的代码';
COMMENT ON TABLE "转诊记录_用药记录" IS '患者转院时关于用药历史明细的记录';


CREATE TABLE IF NOT EXISTS "转科记录" (
"科室代码" varchar (20) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "转科记录流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "新冠感染情况名称" varchar (10) DEFAULT NULL,
 "新冠感染情况代码" varchar (1) DEFAULT NULL,
 "转入医师姓名" varchar (50) DEFAULT NULL,
 "转入医师工号" varchar (20) DEFAULT NULL,
 "转入科室名称" varchar (100) DEFAULT NULL,
 "转入科室代码" varchar (20) DEFAULT NULL,
 "转入时间" timestamp DEFAULT NULL,
 "转出医师姓名" varchar (50) DEFAULT NULL,
 "转出医师工号" varchar (20) DEFAULT NULL,
 "转出科室名称" varchar (100) DEFAULT NULL,
 "转出科室代码" varchar (20) DEFAULT NULL,
 "转出时间" timestamp DEFAULT NULL,
 "转科记录类型名称" decimal (1,
 0) DEFAULT NULL,
 "转科记录类型代码" varchar (1) DEFAULT NULL,
 "注意事项" text,
 "转入诊疗计划" text,
 "转科目的" varchar (200) DEFAULT NULL,
 "中药处方医嘱内容" text,
 "中药用药方法" varchar (100) DEFAULT NULL,
 "中药煎煮方法" varchar (100) DEFAULT NULL,
 "治则治法代码" varchar (100) DEFAULT NULL,
 "中医“四诊”观察结果" text,
 "目前诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "目前诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "目前诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "目前诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "目前诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "目前诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "入院诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "入院诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "入院诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "入院诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "入院诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "入院诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "目前情况" text,
 "诊疗过程描述" text,
 "入院情况" text,
 "主诉" text,
 "入院时间" timestamp DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 CONSTRAINT "转科记录"_"转科记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("转科记录流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "转科记录"."科室名称" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "转科记录"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "转科记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "转科记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "转科记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "转科记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "转科记录"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "转科记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "转科记录"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "转科记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "转科记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "转科记录"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "转科记录"."主诉" IS '对患者本次疾病相关的主要症状及其持续时间的描述，一般由患者本人或监护人描述';
COMMENT ON COLUMN "转科记录"."入院情况" IS '对患者入院情况的详细描述';
COMMENT ON COLUMN "转科记录"."诊疗过程描述" IS '对患者诊疗过程或抢救情况的详细描述';
COMMENT ON COLUMN "转科记录"."目前情况" IS '对患者当前情况的详细描述';
COMMENT ON COLUMN "转科记录"."入院诊断-西医诊断代码" IS '患者入院时按照平台编码规则赋予西医初步诊断疾病的唯一标识';
COMMENT ON COLUMN "转科记录"."入院诊断-西医诊断名称" IS '由医师根据患者入院时的情况，综合分析所作出的西医诊断标准名称';
COMMENT ON COLUMN "转科记录"."入院诊断-中医病名代码" IS '患者入院时按照平台编码规则赋予初步诊断中医疾病的唯一标识';
COMMENT ON COLUMN "转科记录"."入院诊断-中医病名名称" IS '由医师根据患者人院时的情况，综合分析所作出的中医疾病标准名称';
COMMENT ON COLUMN "转科记录"."入院诊断-中医症候代码" IS '患者入院时按照平台编码规则赋予初步诊断中医证候的唯一标识';
COMMENT ON COLUMN "转科记录"."入院诊断-中医症候名称" IS '由医师根据患者人院时的情况，综合分析所作出的标准中医证候名称';
COMMENT ON COLUMN "转科记录"."目前诊断-西医诊断代码" IS '按照特定编码规则赋予当前诊断疾病的唯一标识';
COMMENT ON COLUMN "转科记录"."目前诊断-西医诊断名称" IS '目前诊断西医诊断标准名称';
COMMENT ON COLUMN "转科记录"."目前诊断-中医病名代码" IS '患者入院时按照特定编码规则赋予目前诊断中医疾病的唯一标识';
COMMENT ON COLUMN "转科记录"."目前诊断-中医病名名称" IS '患者入院时按照平台编码规则赋予目前诊断中医疾病的唯一标识';
COMMENT ON COLUMN "转科记录"."目前诊断-中医症候代码" IS '患者入院时按照平台编码规则赋予目前诊断中医证候的唯一标识';
COMMENT ON COLUMN "转科记录"."目前诊断-中医症候名称" IS '目前诊断中医证候标准名称';
COMMENT ON COLUMN "转科记录"."中医“四诊”观察结果" IS '中医“四诊”观察结果的详细描述，包括望、闻、问、切四诊内容';
COMMENT ON COLUMN "转科记录"."治则治法代码" IS '辩证结果采用的治则治法在特定编码体系中的代码。如有多条，用“，”加以分隔';
COMMENT ON COLUMN "转科记录"."中药煎煮方法" IS '中药饮片煎煮方法描述，如水煎等';
COMMENT ON COLUMN "转科记录"."中药用药方法" IS '中药的用药方法的描述，如bid 煎服，先煎、后下等';
COMMENT ON COLUMN "转科记录"."中药处方医嘱内容" IS '中药处方医嘱内容的详细描述';
COMMENT ON COLUMN "转科记录"."转科目的" IS '对患者转科目的详细描述';
COMMENT ON COLUMN "转科记录"."转入诊疗计划" IS '转入诊疗计划，包括具体的检査、中西医治疗措施及中医调护';
COMMENT ON COLUMN "转科记录"."注意事项" IS '对可能出现问题及采取相应措施的描述';
COMMENT ON COLUMN "转科记录"."转科记录类型代码" IS '患者转科记录类别在特定编码体系中的代码';
COMMENT ON COLUMN "转科记录"."转科记录类型名称" IS '患者转科记录的类别描述';
COMMENT ON COLUMN "转科记录"."转出时间" IS '患者转出当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "转科记录"."转出科室代码" IS '按照机构内编码规则赋予转出科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "转科记录"."转出科室名称" IS '转出科室在机构内编码体系中的名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "转科记录"."转出医师工号" IS '转出科室经治医师的工号';
COMMENT ON COLUMN "转科记录"."转出医师姓名" IS '转出科室经治医师本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "转科记录"."转入时间" IS '患者转入到现管理单位的公元纪年日期和时间完整描述';
COMMENT ON COLUMN "转科记录"."转入科室代码" IS '按照机构内编码规则赋予转入科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "转科记录"."转入科室名称" IS '转入科室的机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "转科记录"."转入医师工号" IS '转入科室经治医师的工号';
COMMENT ON COLUMN "转科记录"."转入医师姓名" IS '转入科室经治医师本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "转科记录"."新冠感染情况代码" IS '新冠感染情况(如未查/不详、阴性、阳性等)在特定编码体系中的代码';
COMMENT ON COLUMN "转科记录"."新冠感染情况名称" IS '新冠感染情况(如未查/不详、阴性、阳性等)在特定编码体系中的名称';
COMMENT ON COLUMN "转科记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "转科记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "转科记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "转科记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "转科记录"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名';
COMMENT ON COLUMN "转科记录"."转科记录流水号" IS '按照某一特性编码规则赋予转科记录的唯一标识';
COMMENT ON COLUMN "转科记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "转科记录"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "转科记录"."住院次数" IS '办理完整住院治疗手续的次数';
COMMENT ON COLUMN "转科记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "转科记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "转科记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "转科记录"."科室代码" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的代码';
COMMENT ON TABLE "转科记录" IS '患者转科时，关于患者病情、诊断、治疗经过、转科目的内容的记录';


CREATE TABLE IF NOT EXISTS "设备信息统计表" (
"数据更新时间" timestamp DEFAULT NULL,
 "高流量湿化氧疗系统总数" decimal (5,
 0) DEFAULT NULL,
 "高流量湿化氧疗系统在用数" decimal (5,
 0) DEFAULT NULL,
 "无创呼吸机总数" decimal (5,
 0) DEFAULT NULL,
 "无创呼吸机在用数" decimal (5,
 0) DEFAULT NULL,
 "有创呼吸机总数" decimal (5,
 0) DEFAULT NULL,
 "有创呼吸机在用数" decimal (5,
 0) DEFAULT NULL,
 "CRRT总数" decimal (5,
 0) DEFAULT NULL,
 "CRRT在用数" decimal (5,
 0) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "ECMO在用数" decimal (5,
 0) DEFAULT NULL,
 "ECMO总数" decimal (5,
 0) DEFAULT NULL,
 "在院人数" decimal (5,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "数据上报日期" date NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 CONSTRAINT "设备信息统计表"_"数据上报日期"_"医疗机构代码"_PK PRIMARY KEY ("数据上报日期",
 "医疗机构代码")
);
COMMENT ON COLUMN "设备信息统计表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "设备信息统计表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "设备信息统计表"."数据上报日期" IS '数据上报当日的公元纪年日期';
COMMENT ON COLUMN "设备信息统计表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "设备信息统计表"."在院人数" IS '截止上报当日的在院人数';
COMMENT ON COLUMN "设备信息统计表"."ECMO总数" IS '截止上报当日ECMO总数';
COMMENT ON COLUMN "设备信息统计表"."ECMO在用数" IS '截止上报当日ECMO正在使用的数量';
COMMENT ON COLUMN "设备信息统计表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "设备信息统计表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "设备信息统计表"."CRRT在用数" IS '截止上报当日CRRT正在使用的数量';
COMMENT ON COLUMN "设备信息统计表"."CRRT总数" IS '截止上报当日CRRT总数';
COMMENT ON COLUMN "设备信息统计表"."有创呼吸机在用数" IS '截止上报当日有创呼吸机正在使用的数量';
COMMENT ON COLUMN "设备信息统计表"."有创呼吸机总数" IS '截止上报当日有创呼吸机总数';
COMMENT ON COLUMN "设备信息统计表"."无创呼吸机在用数" IS '截止上报当日无创呼吸机正在使用的数量';
COMMENT ON COLUMN "设备信息统计表"."无创呼吸机总数" IS '截止上报当日无创呼吸机总数';
COMMENT ON COLUMN "设备信息统计表"."高流量湿化氧疗系统在用数" IS '截止上报当日高流量湿化氧疗系统正在使用的数量';
COMMENT ON COLUMN "设备信息统计表"."高流量湿化氧疗系统总数" IS '截止上报当日高流量湿化氧疗系统总数';
COMMENT ON COLUMN "设备信息统计表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON TABLE "设备信息统计表" IS '按日统计医院设备使用情况，包括高流量湿化氧疗系统总数、无创呼吸机总数、有创呼吸机总数等';


CREATE TABLE IF NOT EXISTS "血透治疗记录" (
"透析方式名称" varchar (50) DEFAULT NULL,
 "透析液温度" decimal (4,
 1) DEFAULT NULL,
 "抗凝剂总量" decimal (8,
 0) DEFAULT NULL,
 "抗凝剂维持量" decimal (8,
 0) DEFAULT NULL,
 "血管通路凝血情况" varchar (1) DEFAULT NULL,
 "干体重" decimal (6,
 0) DEFAULT NULL,
 "透析前净体重" decimal (6,
 0) DEFAULT NULL,
 "透析后净体重" decimal (6,
 0) DEFAULT NULL,
 "本次体重增加数量" decimal (6,
 0) DEFAULT NULL,
 "透析液处方钙含量" decimal (6,
 0) DEFAULT NULL,
 "透析液处方钾含量" decimal (6,
 0) DEFAULT NULL,
 "并发症" varchar (200) DEFAULT NULL,
 "透后症状" varchar (100) DEFAULT NULL,
 "舒适度代码" varchar (1) DEFAULT NULL,
 "血管通路类型代码" varchar (1) DEFAULT NULL,
 "小结" varchar (1000) DEFAULT NULL,
 "上机护士姓名" varchar (50) DEFAULT NULL,
 "核对护士姓名" varchar (50) DEFAULT NULL,
 "责任护士姓名" varchar (50) DEFAULT NULL,
 "责任医生工号" varchar (20) DEFAULT NULL,
 "责任医生姓名" varchar (50) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "透析编号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "床位号" varchar (32) DEFAULT NULL,
 "透析日期" date DEFAULT NULL,
 "透析器" varchar (100) DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "脉率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "呼吸频率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "设置超滤量" decimal (8,
 0) DEFAULT NULL,
 "抗凝剂名称" varchar (100) DEFAULT NULL,
 "首次剂量" decimal (8,
 0) DEFAULT NULL,
 "透析器凝血情况代码" varchar (1) DEFAULT NULL,
 "透析机型号" varchar (32) DEFAULT NULL,
 "透析次数" decimal (6,
 0) DEFAULT NULL,
 "透析时长" decimal (6,
 0) DEFAULT NULL,
 "透析方式代码" varchar (10) DEFAULT NULL,
 CONSTRAINT "血透治疗记录"_"医疗机构代码"_"透析编号"_PK PRIMARY KEY ("医疗机构代码",
 "透析编号")
);
COMMENT ON COLUMN "血透治疗记录"."透析方式代码" IS '透析方式在特定编码体系中的代码';
COMMENT ON COLUMN "血透治疗记录"."透析时长" IS '本次透析治疗的总持续时间，计量单位小时';
COMMENT ON COLUMN "血透治疗记录"."透析次数" IS '即“第 次透析”指患者在本医疗机构透析治疗的次数，计量单位为次';
COMMENT ON COLUMN "血透治疗记录"."透析机型号" IS '透析机的型号描述';
COMMENT ON COLUMN "血透治疗记录"."透析器凝血情况代码" IS '透析器凝血情况在特定编码体系中的代码';
COMMENT ON COLUMN "血透治疗记录"."首次剂量" IS '抗凝剂首次使用剂量值，计量单位ml';
COMMENT ON COLUMN "血透治疗记录"."抗凝剂名称" IS '抗凝剂的名称';
COMMENT ON COLUMN "血透治疗记录"."设置超滤量" IS '患者本次透析治疗时设置的超滤量，计量单位ml';
COMMENT ON COLUMN "血透治疗记录"."舒张压(mmHg)" IS '本次透析治疗前的舒张压,单位：mmHg';
COMMENT ON COLUMN "血透治疗记录"."收缩压(mmHg)" IS '本次透析治疗前的收缩压,单位：mmHg';
COMMENT ON COLUMN "血透治疗记录"."呼吸频率(次/min)" IS '本次透析治疗前的呼吸频率，单位：次/分钟';
COMMENT ON COLUMN "血透治疗记录"."脉率(次/min)" IS '本次透析治疗前的脉率，单位：次/分钟';
COMMENT ON COLUMN "血透治疗记录"."体温(℃)" IS '本次透析治疗前的体温，单位：℃';
COMMENT ON COLUMN "血透治疗记录"."透析器" IS '透析器类型、种类等描述';
COMMENT ON COLUMN "血透治疗记录"."透析日期" IS '本次透析完成当日的公元纪年日期';
COMMENT ON COLUMN "血透治疗记录"."床位号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "血透治疗记录"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "血透治疗记录"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "血透治疗记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "血透治疗记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "血透治疗记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "血透治疗记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "血透治疗记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "血透治疗记录"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "血透治疗记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "血透治疗记录"."透析编号" IS '按照一定编码规则赋予本次透析治疗的唯一标识';
COMMENT ON COLUMN "血透治疗记录"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "血透治疗记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "血透治疗记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血透治疗记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血透治疗记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "血透治疗记录"."责任医生姓名" IS '责任医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "血透治疗记录"."责任医生工号" IS '责任医师在原始特定编码体系中的编号';
COMMENT ON COLUMN "血透治疗记录"."责任护士姓名" IS '责任护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "血透治疗记录"."核对护士姓名" IS '核对护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "血透治疗记录"."上机护士姓名" IS '上机护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "血透治疗记录"."小结" IS '本次透析情况的详细描述';
COMMENT ON COLUMN "血透治疗记录"."血管通路类型代码" IS '血管通路类型在特定编码体系中的代码';
COMMENT ON COLUMN "血透治疗记录"."舒适度代码" IS '血液透析舒适度级别在特定编码体系中的代码';
COMMENT ON COLUMN "血透治疗记录"."透后症状" IS '本次透析治疗后，病人症状描述，多项时使用“；”分割';
COMMENT ON COLUMN "血透治疗记录"."并发症" IS '本次透析治疗中的相关并发症描述，多项时使用“；”分割';
COMMENT ON COLUMN "血透治疗记录"."透析液处方钾含量" IS '透析液处方中钾含量，计量单位mmol/L';
COMMENT ON COLUMN "血透治疗记录"."透析液处方钙含量" IS '透析液处方中钙含量，计量单位mmol/L';
COMMENT ON COLUMN "血透治疗记录"."本次体重增加数量" IS '本次透析治疗体重增加数量，计量单位kg';
COMMENT ON COLUMN "血透治疗记录"."透析后净体重" IS '透析后净体重，计量单位kg';
COMMENT ON COLUMN "血透治疗记录"."透析前净体重" IS '透析前净体重，计量单位kg';
COMMENT ON COLUMN "血透治疗记录"."干体重" IS '本次透析治疗的目标体重，计量单位kg';
COMMENT ON COLUMN "血透治疗记录"."血管通路凝血情况" IS '血管通路凝血情况在特定编码体系中的代码';
COMMENT ON COLUMN "血透治疗记录"."抗凝剂维持量" IS '本次透析治疗抗凝剂的最小药物剂量，剂量单位ml';
COMMENT ON COLUMN "血透治疗记录"."抗凝剂总量" IS '本次透析治疗需要的抗凝剂总量，计量单位ml';
COMMENT ON COLUMN "血透治疗记录"."透析液温度" IS '透析液温度，计量单位℃';
COMMENT ON COLUMN "血透治疗记录"."透析方式名称" IS '透析方式在特定编码体系中的名称';
COMMENT ON TABLE "血透治疗记录" IS '患者血液透析治疗结果记录，包括透析前后各项指标结果，透析时长、方式、小结信息';


CREATE TABLE IF NOT EXISTS "血袋回收记录表" (
"医疗机构名称" varchar (70) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "记录时间" timestamp DEFAULT NULL,
 "回收人员姓名" varchar (50) DEFAULT NULL,
 "回收原因" varchar (1000) DEFAULT NULL,
 "回收日期" date DEFAULT NULL,
 "规格" varchar (255) DEFAULT NULL,
 "血液品种代码" varchar (5) DEFAULT NULL,
 "血袋编号" varchar (50) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "回收流水号" varchar (36) NOT NULL,
 CONSTRAINT "血袋回收记录表"_"医疗机构代码"_"回收流水号"_PK PRIMARY KEY ("医疗机构代码",
 "回收流水号")
);
COMMENT ON COLUMN "血袋回收记录表"."回收流水号" IS '按照某一特定编码规则赋予血袋回收记录的顺序号，是血袋回收记录的唯一标识';
COMMENT ON COLUMN "血袋回收记录表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "血袋回收记录表"."血袋编号" IS '全省唯一血袋编码，来源于血站';
COMMENT ON COLUMN "血袋回收记录表"."血液品种代码" IS '全血或血液成分类别(如红细胞、全血、血小板、血浆等)在特定编码体系中的代码';
COMMENT ON COLUMN "血袋回收记录表"."规格" IS '血袋规格描述';
COMMENT ON COLUMN "血袋回收记录表"."回收日期" IS '血袋回收的公元纪年日期';
COMMENT ON COLUMN "血袋回收记录表"."回收原因" IS '血袋回收原因的详细描述';
COMMENT ON COLUMN "血袋回收记录表"."回收人员姓名" IS '回收人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "血袋回收记录表"."记录时间" IS '完成记录时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血袋回收记录表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "血袋回收记录表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血袋回收记录表"."医疗机构代码" IS '按照某一特定编码规则赋予血袋回收记录的唯一标识';
COMMENT ON COLUMN "血袋回收记录表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血袋回收记录表"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON TABLE "血袋回收记录表" IS '医院血袋回收记录，包括血袋编号、血液种类、规格';


CREATE TABLE IF NOT EXISTS "药敏结果表" (
"就诊科室名称" varchar (100) DEFAULT NULL,
 "申请单号" varchar (32) DEFAULT NULL,
 "报告时间" timestamp DEFAULT NULL,
 "细菌代码" varchar (32) DEFAULT NULL,
 "细菌名称" varchar (128) DEFAULT NULL,
 "打印序号" decimal (10,
 2) DEFAULT NULL,
 "药敏代码" varchar (40) DEFAULT NULL,
 "药敏名称" varchar (40) DEFAULT NULL,
 "抗生素等级" varchar (50) DEFAULT NULL,
 "检测结果" varchar (20) DEFAULT NULL,
 "抗药结果代码" varchar (1) DEFAULT NULL,
 "参考值" varchar (20) DEFAULT NULL,
 "检验方法" varchar (50) DEFAULT NULL,
 "检验机构名称" varchar (700) DEFAULT NULL,
 "纸片含药量" varchar (16) DEFAULT NULL,
 "纸片含药量单位" varchar (10) DEFAULT NULL,
 "抑菌浓度" varchar (10) DEFAULT NULL,
 "抑菌环直径" varchar (10) DEFAULT NULL,
 "试验板序号" decimal (10,
 2) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "药敏结果流水号" varchar (128) NOT NULL,
 "医疗机构名称" varchar (22) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "检验报告流水号" varchar (64) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 CONSTRAINT "药敏结果表"_"药敏结果流水号"_"医疗机构代码"_PK PRIMARY KEY ("药敏结果流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "药敏结果表"."就诊科室代码" IS '就诊科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在机构内编码体系中的代码';
COMMENT ON COLUMN "药敏结果表"."检验报告流水号" IS '按照某一特定编码规则赋予检验报告记录的唯一标识，在同一家医院内唯一';
COMMENT ON COLUMN "药敏结果表"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "药敏结果表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "药敏结果表"."医疗机构名称" IS '就诊医疗机构的名称';
COMMENT ON COLUMN "药敏结果表"."药敏结果流水号" IS '按照某一特定编码规则赋予每条药敏试验结果的顺序号';
COMMENT ON COLUMN "药敏结果表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "药敏结果表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "药敏结果表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "药敏结果表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "药敏结果表"."试验板序号" IS '试验板在特定编码体系中的编号';
COMMENT ON COLUMN "药敏结果表"."抑菌环直径" IS '抑菌环的直径大小，通常计量单位为mm';
COMMENT ON COLUMN "药敏结果表"."抑菌浓度" IS '体外培养细菌一定时间后能抑制培养基内细菌生产的药物浓度';
COMMENT ON COLUMN "药敏结果表"."纸片含药量单位" IS '药物使用剂量单位，如mg，ml等';
COMMENT ON COLUMN "药敏结果表"."纸片含药量" IS '使用药物的剂量值，按剂量单位计';
COMMENT ON COLUMN "药敏结果表"."检验机构名称" IS '检验医疗机构在原始体系中的组织机构名称';
COMMENT ON COLUMN "药敏结果表"."检验方法" IS '药敏试验项目所采用的检验方法名称，如罗氏发光免疫、雅培发光免疫等';
COMMENT ON COLUMN "药敏结果表"."参考值" IS '对结果正常参考值的详细描述';
COMMENT ON COLUMN "药敏结果表"."抗药结果代码" IS '抗药结果在机构内编码体系中的代码';
COMMENT ON COLUMN "药敏结果表"."检测结果" IS '药敏试验结果的详细描述，包括定量结果和定性结果';
COMMENT ON COLUMN "药敏结果表"."抗生素等级" IS '抗菌药物等级的机构内名称，如非限制使用、限制使用、特殊使用';
COMMENT ON COLUMN "药敏结果表"."药敏名称" IS '平台统一的抗生素名称。例如：“阿莫西林”';
COMMENT ON COLUMN "药敏结果表"."药敏代码" IS '平台统一的抗生素代码。例如：阿莫西林的代码为“AMX”或“AMOX”';
COMMENT ON COLUMN "药敏结果表"."打印序号" IS '按照某一特定编码规则赋予药敏结果的排序号';
COMMENT ON COLUMN "药敏结果表"."细菌名称" IS '细菌在机构内编码体系中的名称';
COMMENT ON COLUMN "药敏结果表"."细菌代码" IS '细菌在机构内编码体系中的代码';
COMMENT ON COLUMN "药敏结果表"."报告时间" IS '报告生成当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "药敏结果表"."申请单号" IS '按照某一特定编码规则赋予申请单的顺序号';
COMMENT ON COLUMN "药敏结果表"."就诊科室名称" IS '就诊科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在机构内编码体系中的名称';
COMMENT ON TABLE "药敏结果表" IS '药敏实验结果信息';


CREATE TABLE IF NOT EXISTS "细菌结果表" (
"细菌代码" varchar (32) DEFAULT NULL,
 "检验报告流水号" varchar (64) DEFAULT NULL,
 "电子申请单编号" varchar (100) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "细菌结果流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (22) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "报告时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "试验板名称" varchar (50) DEFAULT NULL,
 "试验板序号" decimal (10,
 2) DEFAULT NULL,
 "仪器名称" varchar (100) DEFAULT NULL,
 "仪器编号" varchar (20) DEFAULT NULL,
 "设备类别代码" varchar (20) DEFAULT NULL,
 "检测结果文字描述" varchar (1024) DEFAULT NULL,
 "检测结果" varchar (20) DEFAULT NULL,
 "发现方式" varchar (64) DEFAULT NULL,
 "培养条件" varchar (64) DEFAULT NULL,
 "培养时长" varchar (16) DEFAULT NULL,
 "培养基" varchar (40) DEFAULT NULL,
 "菌落计数" varchar (16) DEFAULT NULL,
 "细菌名称" varchar (128) DEFAULT NULL,
 CONSTRAINT "细菌结果表"_"细菌结果流水号"_"医疗机构代码"_PK PRIMARY KEY ("细菌结果流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "细菌结果表"."细菌名称" IS '细菌在特定编码体系中的名称';
COMMENT ON COLUMN "细菌结果表"."菌落计数" IS '观察到的菌落数量，计量单位为cfu/ml。如“>10万”';
COMMENT ON COLUMN "细菌结果表"."培养基" IS '培养基类型，如：巧克力平板、血平板、琼脂平板等';
COMMENT ON COLUMN "细菌结果表"."培养时长" IS '菌落培养时长，包含单位。例如：72小时';
COMMENT ON COLUMN "细菌结果表"."培养条件" IS '菌落培养条件描述，例如：“37℃；空气”或“35℃±2℃；5%CO2”';
COMMENT ON COLUMN "细菌结果表"."发现方式" IS '细菌发现方式描述，例如：“肉眼”或“镜检”等';
COMMENT ON COLUMN "细菌结果表"."检测结果" IS '简要描述是否发现或是否生长等';
COMMENT ON COLUMN "细菌结果表"."检测结果文字描述" IS '详细描述检验的结果';
COMMENT ON COLUMN "细菌结果表"."设备类别代码" IS '检验设备类别的描述';
COMMENT ON COLUMN "细菌结果表"."仪器编号" IS '检验设备在特定编码体系中的编号';
COMMENT ON COLUMN "细菌结果表"."仪器名称" IS '检验设备的名称';
COMMENT ON COLUMN "细菌结果表"."试验板序号" IS '试验板在特定编码体系中的编号';
COMMENT ON COLUMN "细菌结果表"."试验板名称" IS '试验板的名称';
COMMENT ON COLUMN "细菌结果表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "细菌结果表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "细菌结果表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "细菌结果表"."报告时间" IS '报告生成当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "细菌结果表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "细菌结果表"."医疗机构名称" IS '就诊医疗机构的名称';
COMMENT ON COLUMN "细菌结果表"."细菌结果流水号" IS '按照某一特定编码规则赋予细菌结果记录的唯一标识';
COMMENT ON COLUMN "细菌结果表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "细菌结果表"."就诊科室代码" IS '就诊科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在机构内编码体系中的代码';
COMMENT ON COLUMN "细菌结果表"."就诊科室名称" IS '就诊科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在机构内编码体系中的名称';
COMMENT ON COLUMN "细菌结果表"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "细菌结果表"."电子申请单编号" IS '按照某一特定编码规则赋予电子申请单的顺序号';
COMMENT ON COLUMN "细菌结果表"."检验报告流水号" IS '按照某一特定编码规则赋予检验报告记录的唯一标识，在同一家医院内唯一';
COMMENT ON COLUMN "细菌结果表"."细菌代码" IS '细菌在特定编码体系中的代码';
COMMENT ON TABLE "细菌结果表" IS '细菌检验结果信息';


CREATE TABLE IF NOT EXISTS "监管统计表" (
"数据日期" date NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "总在院数" decimal (4,
 0) DEFAULT NULL,
 "呼吸专科ICU在院数" decimal (4,
 0) DEFAULT NULL,
 "儿科在院数" decimal (4,
 0) DEFAULT NULL,
 "日间手术量" decimal (4,
 0) DEFAULT NULL,
 "ICU总在院数" decimal (4,
 0) DEFAULT NULL,
 "综合ICU在院数" decimal (4,
 0) DEFAULT NULL,
 "PICU在院数" decimal (4,
 0) DEFAULT NULL,
 "NICU在院数" decimal (4,
 0) DEFAULT NULL,
 "其他专科ICU在院数" decimal (4,
 0) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 CONSTRAINT "监管统计表"_"数据日期"_"医疗机构代码"_PK PRIMARY KEY ("数据日期",
 "医疗机构代码")
);
COMMENT ON COLUMN "监管统计表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "监管统计表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "监管统计表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "监管统计表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "监管统计表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "监管统计表"."其他专科ICU在院数" IS '截止统计当日其他专科ICU在院';
COMMENT ON COLUMN "监管统计表"."NICU在院数" IS '截止统计当日NICU在院人数';
COMMENT ON COLUMN "监管统计表"."PICU在院数" IS '截止统计当日PICU在院在院人数';
COMMENT ON COLUMN "监管统计表"."综合ICU在院数" IS '截止统计当日综合ICU在院人数';
COMMENT ON COLUMN "监管统计表"."ICU总在院数" IS '截止统计当日ICU总在院人数';
COMMENT ON COLUMN "监管统计表"."日间手术量" IS '截止统计当日的日间手术量';
COMMENT ON COLUMN "监管统计表"."儿科在院数" IS '截止统计当日的儿科在院人数';
COMMENT ON COLUMN "监管统计表"."呼吸专科ICU在院数" IS '截止统计当日呼吸专科ICU在院人数';
COMMENT ON COLUMN "监管统计表"."总在院数" IS '截止统计当日的总在院人数';
COMMENT ON COLUMN "监管统计表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "监管统计表"."数据日期" IS '数据统计当日的公元纪年日期';
COMMENT ON TABLE "监管统计表" IS '按日统计医院icu业务监管数据，如总在院数、儿科在院数、日间手术量、ICU总在院数等';


CREATE TABLE IF NOT EXISTS "病案首页其他诊断" (
"数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "诊断序号" varchar (2) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "住院就诊流水号" varchar (32) NOT NULL,
 "首页序号" varchar (50) DEFAULT NULL,
 "诊断代码" varchar (36) DEFAULT NULL,
 "诊断名称" varchar (256) DEFAULT NULL,
 "诊断分类代码" varchar (1) DEFAULT NULL,
 "肿瘤代码" varchar (20) DEFAULT NULL,
 "肿瘤名称" varchar (256) DEFAULT NULL,
 "最高诊断依据代码" varchar (2) DEFAULT NULL,
 "入院病情代码" varchar (4) DEFAULT NULL,
 "出院情况" text,
 "更新时间" timestamp DEFAULT NULL,
 "分院代码" varchar (2) DEFAULT NULL,
 "分院名称" varchar (120) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 CONSTRAINT "病案首页其他诊断"_"医疗机构代码"_"诊断序号"_"住院就诊流水号"_PK PRIMARY KEY ("医疗机构代码",
 "诊断序号",
 "住院就诊流水号")
);
COMMENT ON COLUMN "病案首页其他诊断"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病案首页其他诊断"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "病案首页其他诊断"."分院名称" IS '分院的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "病案首页其他诊断"."分院代码" IS '按照某一特定编码规则赋予分院的唯一标识。记录分院代码（内码）；如无分院，则传0';
COMMENT ON COLUMN "病案首页其他诊断"."更新时间" IS '数据更新完成当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病案首页其他诊断"."出院情况" IS '患者所患的每种疾病的治疗结果类别(如治愈、好转、稳定、恶化等)在特定编码体系中的代码';
COMMENT ON COLUMN "病案首页其他诊断"."入院病情代码" IS '入院病情评估情况(如临床未确定、情况不明等)在特定编码体系中的代码';
COMMENT ON COLUMN "病案首页其他诊断"."最高诊断依据代码" IS '患者最高诊断依据(如病理、手术、内镜、血管造影、CT等)在特定编码体系中的代码';
COMMENT ON COLUMN "病案首页其他诊断"."肿瘤名称" IS '肿瘤诊断在平台编码规则体系中名称';
COMMENT ON COLUMN "病案首页其他诊断"."肿瘤代码" IS '肿瘤诊断在平台编码规则体系中唯一标识';
COMMENT ON COLUMN "病案首页其他诊断"."诊断分类代码" IS '诊断分类(如西医诊断、中医症候、中医疾病等)在特定编码体系中的代码';
COMMENT ON COLUMN "病案首页其他诊断"."诊断名称" IS '疾病诊断在特定编码体系中的名称。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "病案首页其他诊断"."诊断代码" IS '疾病诊断在特定编码体系中的代码。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "病案首页其他诊断"."首页序号" IS '按照某一特定编码规则赋予病案首页的排序号';
COMMENT ON COLUMN "病案首页其他诊断"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "病案首页其他诊断"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "病案首页其他诊断"."诊断序号" IS '诊断的排序号，序号1一般指的为主要诊断';
COMMENT ON COLUMN "病案首页其他诊断"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "病案首页其他诊断"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "病案首页其他诊断"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON TABLE "病案首页其他诊断" IS '住院病案首页诊断明细信息';


CREATE TABLE IF NOT EXISTS "病室工作日志明细记录" (
"手术例数" decimal (5,
 0) DEFAULT NULL,
 "何科转入" varchar (100) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病室工作日志明细序号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病室工作日志记录日期" date DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "入院与出院病人标志" varchar (1) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "病室工作日志序号" varchar (32) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "护士/医生姓名" varchar (50) DEFAULT NULL,
 "转往何科" varchar (100) DEFAULT NULL,
 "死亡标志" varchar (1) DEFAULT NULL,
 CONSTRAINT "病室工作日志明细记录"_"病室工作日志明细序号"_"医疗机构代码"_"病室工作日志序号"_PK PRIMARY KEY ("病室工作日志明细序号",
 "医疗机构代码",
 "病室工作日志序号")
);
COMMENT ON COLUMN "病室工作日志明细记录"."死亡标志" IS '本次不良反应事件中患者是否死亡的标志';
COMMENT ON COLUMN "病室工作日志明细记录"."转往何科" IS '转出患者将转入的科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "病室工作日志明细记录"."护士/医生姓名" IS '护士/医生在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "病室工作日志明细记录"."签名时间" IS '签名完成时的公元纪年和日期的完整描述';
COMMENT ON COLUMN "病室工作日志明细记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "病室工作日志明细记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病室工作日志明细记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病室工作日志明细记录"."病室工作日志序号" IS '按照某一特定编码规则赋予病室工作日志记录的顺序号，是病室工作日志记录的唯一标识';
COMMENT ON COLUMN "病室工作日志明细记录"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "病室工作日志明细记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "病室工作日志明细记录"."姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "病室工作日志明细记录"."入院与出院病人标志" IS '患者是否为入院或者转入患者的标志';
COMMENT ON COLUMN "病室工作日志明细记录"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "病室工作日志明细记录"."病室工作日志记录日期" IS '记录完成时的公元纪年日期的描述';
COMMENT ON COLUMN "病室工作日志明细记录"."病区名称" IS '患者当前所在病区在特定编码体系中的名称';
COMMENT ON COLUMN "病室工作日志明细记录"."就诊科室名称" IS '所在科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "病室工作日志明细记录"."就诊科室代码" IS '按照机构内编码规则赋予所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "病室工作日志明细记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "病室工作日志明细记录"."病室工作日志明细序号" IS '按照某一特定编码规则赋予每条病室工作日志记录明细的顺序号';
COMMENT ON COLUMN "病室工作日志明细记录"."病床号" IS '按照某一特定编码规则赋予患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "病室工作日志明细记录"."何科转入" IS '转入患者转入前所在科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "病室工作日志明细记录"."手术例数" IS '实施手术的次数';
COMMENT ON TABLE "病室工作日志明细记录" IS '病室工作日志明细，包括患者姓名、转入时间、手术例数、转往科室等';


CREATE TABLE IF NOT EXISTS "病危(
重)护理记录" ("年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "饮食情况代码" varchar (2) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "饮食情况名称" varchar (50) DEFAULT NULL,
 "呼吸机监护项目" varchar (20) DEFAULT NULL,
 "护理观察项目名称" varchar (200) DEFAULT NULL,
 "护理观察结果" text,
 "护理操作名称" varchar (100) DEFAULT NULL,
 "护理操作项目类目名称" varchar (100) DEFAULT NULL,
 "护理操作结果" text,
 "护士工号" varchar (20) DEFAULT NULL,
 "护士姓名" varchar (50) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "过敏史" text,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "疾病诊断名称" varchar (512) DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "呼吸频率(次/min)" decimal (3,
 0) DEFAULT NULL,
 "心率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "血糖值(mmol/L)" decimal (6,
 2) DEFAULT NULL,
 "护理等级代码" varchar (1) DEFAULT NULL,
 "护理等级名称" varchar (20) DEFAULT NULL,
 "护理类型代码" varchar (1) DEFAULT NULL,
 "护理类型名称" varchar (20) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "病危(重)护理记录流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 CONSTRAINT "病危(重)护理记录"_"医疗机构代码"_"病危(重)护理记录流水号"_PK PRIMARY KEY ("医疗机构代码",
 "病危(重)护理记录流水号")
);
COMMENT ON COLUMN "病危(重)护理记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "病危(重)护理记录"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "病危(重)护理记录"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "病危(重)护理记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "病危(重)护理记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "病危(重)护理记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "病危(重)护理记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "病危(重)护理记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "病危(重)护理记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "病危(重)护理记录"."就诊次数" IS '对于门(急)诊患者，此处表示患者门(急)诊的累计次数；对于住院患者，此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "病危(重)护理记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "病危(重)护理记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "病危(重)护理记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "病危(重)护理记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "病危(重)护理记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "病危(重)护理记录"."病危(重)护理记录流水号" IS '按照某一特定编码规则赋予病危(重)护理记录的顺序号';
COMMENT ON COLUMN "病危(重)护理记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "病危(重)护理记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "病危(重)护理记录"."护理类型名称" IS '护理类型的分类名称';
COMMENT ON COLUMN "病危(重)护理记录"."护理类型代码" IS '护理类型的分类在特定编码体系中的代码';
COMMENT ON COLUMN "病危(重)护理记录"."护理等级名称" IS '护理级别的分类名称';
COMMENT ON COLUMN "病危(重)护理记录"."护理等级代码" IS '护理级别的分类在特定编码体系中的代码';
COMMENT ON COLUMN "病危(重)护理记录"."血糖值(mmol/L)" IS '血液中葡萄糖定量检测结果值，计量单位为mmol/L';
COMMENT ON COLUMN "病危(重)护理记录"."心率(次/min)" IS '心脏搏动频率的测量值,计量单位为次/min';
COMMENT ON COLUMN "病危(重)护理记录"."呼吸频率(次/min)" IS '受检者单位时间内呼吸的次数，计量单位为次/min';
COMMENT ON COLUMN "病危(重)护理记录"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "病危(重)护理记录"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "病危(重)护理记录"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "病危(重)护理记录"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "病危(重)护理记录"."疾病诊断名称" IS '疾病诊断在特定编码体系中的名称';
COMMENT ON COLUMN "病危(重)护理记录"."疾病诊断代码" IS '疾病诊断在特定编码体系中的代码';
COMMENT ON COLUMN "病危(重)护理记录"."过敏史" IS '患者既往发生过敏情况的详细描述';
COMMENT ON COLUMN "病危(重)护理记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "病危(重)护理记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病危(重)护理记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病危(重)护理记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "病危(重)护理记录"."签名时间" IS '护理护士在护理记录上完成签名的公元纪年和日期的完整描述';
COMMENT ON COLUMN "病危(重)护理记录"."护士姓名" IS '护理护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "病危(重)护理记录"."护士工号" IS '护理护士的工号';
COMMENT ON COLUMN "病危(重)护理记录"."护理操作结果" IS '护理操作结果的详细描述';
COMMENT ON COLUMN "病危(重)护理记录"."护理操作项目类目名称" IS '多个护理操作项目的名称';
COMMENT ON COLUMN "病危(重)护理记录"."护理操作名称" IS '进行护理操作的具体名称';
COMMENT ON COLUMN "病危(重)护理记录"."护理观察结果" IS '对护理观察项目结果的详细描述';
COMMENT ON COLUMN "病危(重)护理记录"."护理观察项目名称" IS '护理观察项目的名称，如患者神志状态、饮食情况，皮肤情况、氧疗情况、排尿排便情况，流量、出量、人量等等，根据护理内容的不同选择不同的观察项目名称';
COMMENT ON COLUMN "病危(重)护理记录"."呼吸机监护项目" IS '对呼吸机监测项目的描述';
COMMENT ON COLUMN "病危(重)护理记录"."饮食情况名称" IS '个体饮食情况所属类别的标准名称，如良好、一般、较差等';
COMMENT ON COLUMN "病危(重)护理记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "病危(重)护理记录"."饮食情况代码" IS '个体饮食情况所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "病危(重)护理记录"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON TABLE "病危(重)护理记录" IS '病危重患者护理记录，包括呼吸机监控项目、护理操作、护理观察项目、护理指标结果信息';


CREATE TABLE IF NOT EXISTS "疑难病例讨论" (
"中药用药方法" varchar (100) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "讨论时间" timestamp DEFAULT NULL,
 "讨论地点" varchar (50) DEFAULT NULL,
 "参加讨论人员名单" varchar (1000) DEFAULT NULL,
 "主持人姓名" varchar (50) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "疑难病例讨论流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "文本内容" text,
 "签名时间" timestamp DEFAULT NULL,
 "主任医师姓名" varchar (50) DEFAULT NULL,
 "主任医师工号" varchar (20) DEFAULT NULL,
 "主治医师姓名" varchar (50) DEFAULT NULL,
 "主治医师工号" varchar (20) DEFAULT NULL,
 "医师工号" varchar (20) DEFAULT NULL,
 "医师姓名" varchar (50) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "讨论意见" text,
 "主持人总结意见" text,
 "中医“四诊”观察结果" text,
 "辨证论治详细描述" text,
 "中药处方医嘱内容" text,
 CONSTRAINT "疑难病例讨论"_"疑难病例讨论流水号"_"医疗机构代码"_PK PRIMARY KEY ("疑难病例讨论流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "疑难病例讨论"."中药处方医嘱内容" IS '中药饮片煎煮方法描述，如水煎等';
COMMENT ON COLUMN "疑难病例讨论"."辨证论治详细描述" IS '对辨证分型的名称、主要依据和采用的治则治法的详细描述';
COMMENT ON COLUMN "疑难病例讨论"."中医“四诊”观察结果" IS '中医“四诊”观察结果的详细描述，包括望、闻、问、切四诊内容';
COMMENT ON COLUMN "疑难病例讨论"."主持人总结意见" IS '主持人就讨论过程总结意见的详细描述';
COMMENT ON COLUMN "疑难病例讨论"."讨论意见" IS '讨论过程中的具体讨论意见的详细描述';
COMMENT ON COLUMN "疑难病例讨论"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "疑难病例讨论"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "疑难病例讨论"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "疑难病例讨论"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "疑难病例讨论"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "疑难病例讨论"."医师姓名" IS '记录医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "疑难病例讨论"."医师工号" IS '记录医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "疑难病例讨论"."主治医师工号" IS '所在科室的具有主治医师专业技术职务资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "疑难病例讨论"."主治医师姓名" IS '所在科室的具有主治医师的工号';
COMMENT ON COLUMN "疑难病例讨论"."主任医师工号" IS '主任医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "疑难病例讨论"."主任医师姓名" IS '主任医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "疑难病例讨论"."签名时间" IS '医师姓名当天的的公元纪年日期的完整描述';
COMMENT ON COLUMN "疑难病例讨论"."文本内容" IS '存入大文本的内容，最大不超过64K';
COMMENT ON COLUMN "疑难病例讨论"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "疑难病例讨论"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "疑难病例讨论"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "疑难病例讨论"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "疑难病例讨论"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "疑难病例讨论"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名';
COMMENT ON COLUMN "疑难病例讨论"."疑难病例讨论流水号" IS '按照一定编码规则赋予疑难病例讨论记录的唯一标识';
COMMENT ON COLUMN "疑难病例讨论"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "疑难病例讨论"."性别名称" IS '一般指患者医学生理性别，指男性或女性';
COMMENT ON COLUMN "疑难病例讨论"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "疑难病例讨论"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "疑难病例讨论"."主持人姓名" IS '讨论记录主持人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "疑难病例讨论"."参加讨论人员名单" IS '参与讨论人员的姓名列表，以”，“分隔';
COMMENT ON COLUMN "疑难病例讨论"."讨论地点" IS '诊疗相关讨论所在地点的具体描述';
COMMENT ON COLUMN "疑难病例讨论"."讨论时间" IS '讨论记录开始时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "疑难病例讨论"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "疑难病例讨论"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "疑难病例讨论"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "疑难病例讨论"."科室名称" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "疑难病例讨论"."科室代码" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "疑难病例讨论"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "疑难病例讨论"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "疑难病例讨论"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "疑难病例讨论"."就诊次数" IS '患者在该医疗机构就诊的次数';
COMMENT ON COLUMN "疑难病例讨论"."中药用药方法" IS '中药的用药方法的描述，如bid 煎服，先煎、后下等';
COMMENT ON TABLE "疑难病例讨论" IS '疑难病例讨论医师意见、诊治方法、用药等医嘱内容的记录';


CREATE TABLE IF NOT EXISTS "生殖健康信息" (
"数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "全员人口个案标识号" varchar (64) NOT NULL,
 "流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "妇女姓名" varchar (50) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "孕情检查结果" varchar (1) DEFAULT NULL,
 "孕情检查时间" timestamp DEFAULT NULL,
 "孕情检查机构" varchar (70) DEFAULT NULL,
 "宫内节育器检查结果" varchar (1) DEFAULT NULL,
 "宫内节育器检查时间" timestamp DEFAULT NULL,
 "宫内节育器检查机构" varchar (70) DEFAULT NULL,
 "生殖健康检查结果代码" varchar (3) DEFAULT NULL,
 "生殖健康器检查时间" timestamp DEFAULT NULL,
 "生殖健康检查机构" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 CONSTRAINT "生殖健康信息"_"医疗机构代码"_"全员人口个案标识号"_"流水号"_PK PRIMARY KEY ("医疗机构代码",
 "全员人口个案标识号",
 "流水号")
);
COMMENT ON COLUMN "生殖健康信息"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "生殖健康信息"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "生殖健康信息"."生殖健康检查机构" IS '生殖健康检查机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "生殖健康信息"."生殖健康器检查时间" IS '生殖健康检查时的公元纪年日期';
COMMENT ON COLUMN "生殖健康信息"."生殖健康检查结果代码" IS '生殖健康检查结果的类别在特定编码体系中的代码';
COMMENT ON COLUMN "生殖健康信息"."宫内节育器检查机构" IS '宫内节育器检查机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "生殖健康信息"."宫内节育器检查时间" IS '宫内节育器检查时的公元纪年日期';
COMMENT ON COLUMN "生殖健康信息"."宫内节育器检查结果" IS '宫内节育器检查结果的类别代码';
COMMENT ON COLUMN "生殖健康信息"."孕情检查机构" IS '孕情检查机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "生殖健康信息"."孕情检查时间" IS '孕情检查时的公元纪年日期';
COMMENT ON COLUMN "生殖健康信息"."孕情检查结果" IS '标识妇女是否怀孕的标志';
COMMENT ON COLUMN "生殖健康信息"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "生殖健康信息"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "生殖健康信息"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "生殖健康信息"."妇女姓名" IS '妇女本人在公安户籍管理部门正式登记注册的姓氏和名称。未取名者填“C”+生母姓名';
COMMENT ON COLUMN "生殖健康信息"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "生殖健康信息"."流水号" IS '按照某一特定编码规则赋予生育记录的唯一标识号';
COMMENT ON COLUMN "生殖健康信息"."全员人口个案标识号" IS '按照某一特定编码规则赋予全员人口个案的唯一标识号';
COMMENT ON COLUMN "生殖健康信息"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "生殖健康信息"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "生殖健康信息"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON TABLE "生殖健康信息" IS '妇女生殖健康信息，包括孕情检查、宫内节育器检查、生殖健康检查等';


CREATE TABLE IF NOT EXISTS "特殊检查及特殊治疗同意书" (
"医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "特殊检查及特殊治疗同意书流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "门诊就诊流水号" varchar (32) DEFAULT NULL,
 "知情同意书编号" varchar (20) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "疾病诊断名称" varchar (512) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "特殊检查及特殊治疗项目名称" varchar (100) DEFAULT NULL,
 "特殊检查及特殊治疗目的" varchar (100) DEFAULT NULL,
 "特殊检查及特殊治疗可能引起的并发症及风险" text,
 "替代方案" text,
 "患者/法定代理人姓名" varchar (50) DEFAULT NULL,
 "法定代理人与患者的关系代码" varchar (1) DEFAULT NULL,
 "法定代理人与患者的关系名称" varchar (100) DEFAULT NULL,
 "患者/法定代理人签名时间" timestamp DEFAULT NULL,
 "医疗机构意见" text,
 "医师工号" varchar (20) DEFAULT NULL,
 "医师姓名" varchar (50) DEFAULT NULL,
 "医师签名时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "特殊检查及特殊治疗同意书"_"医疗机构代码"_"特殊检查及特殊治疗同意书流水号"_PK PRIMARY KEY ("医疗机构代码",
 "特殊检查及特殊治疗同意书流水号")
);
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."医师签名时间" IS '医师进行电子签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."医师姓名" IS '医师签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."医师工号" IS '医师的工号';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."医疗机构意见" IS '在此诊疗活动过程中，医疗机构对患者应尽责任的陈述以及对可能面临的风险或意外情况所采取的应对措施的详细描述';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."患者/法定代理人签名时间" IS '患者或法定代理人签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."法定代理人与患者的关系名称" IS '本人与特定对象的关系类别(如户主、配偶、子女、父母等)名称';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."法定代理人与患者的关系代码" IS '本人与特定对象的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."患者/法定代理人姓名" IS '患者／法定代理人签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."替代方案" IS '医生即将为患者实施的手术或有创性操作方案之外的其他方案，供患者选择';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."特殊检查及特殊治疗可能引起的并发症及风险" IS '拟进行的特殊检查及特殊治疗项目可能引起的并发症及风险描述';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."特殊检查及特殊治疗目的" IS '拟进行的特殊检查以及特殊治疗的目的描述';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."特殊检查及特殊治疗项目名称" IS '拟进行的特殊检查及特殊治疗项目名称的描述';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."性别名称" IS '个体生理性别名称';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."就诊次数" IS '对于门(急)诊患者，此处表示患者门(急)诊的累计次数；对于住院患者，此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."疾病诊断名称" IS '疾病诊断在特定编码体系中的名称';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."疾病诊断代码" IS '疾病诊断在特定编码体系中的代码';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."知情同意书编号" IS '按照某一特定编码规则赋予知情同意书的唯一标识';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."门诊就诊流水号" IS '为门诊就诊时，填写门诊就诊流水号，住院就诊流水号填“-”';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."特殊检查及特殊治疗同意书流水号" IS '按照某一特定编码规则赋予特殊检查及特殊治疗同意书的唯一标识';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "特殊检查及特殊治疗同意书"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON TABLE "特殊检查及特殊治疗同意书" IS '特殊治疗项目、可能引起的并发症、替代方案信息的说明';


CREATE TABLE IF NOT EXISTS "治疗用药记录" (
"用药途径代码" varchar (4) DEFAULT NULL,
 "用药明细流水号" varchar (64) NOT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "治疗记录流水号" varchar (64) DEFAULT NULL,
 "药物名称" varchar (50) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "药品用法" varchar (32) DEFAULT NULL,
 "中药类别代码" varchar (2) DEFAULT NULL,
 "用药频次代码" varchar (32) DEFAULT NULL,
 "用药频次名称" varchar (32) DEFAULT NULL,
 "药物剂型代码" varchar (4) DEFAULT NULL,
 "每次使用剂量" decimal (15,
 3) DEFAULT NULL,
 "使用剂量单位" varchar (64) DEFAULT NULL,
 "药物使用总剂量" decimal (11,
 2) DEFAULT NULL,
 CONSTRAINT "治疗用药记录"_"用药明细流水号"_"医疗机构代码"_PK PRIMARY KEY ("用药明细流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "治疗用药记录"."药物使用总剂量" IS '在一定时间段内使用药物的总量。根据单次剂量、频次计算得到的总量';
COMMENT ON COLUMN "治疗用药记录"."使用剂量单位" IS '使用次剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "治疗用药记录"."每次使用剂量" IS '单次使用的剂量，按剂量单位计';
COMMENT ON COLUMN "治疗用药记录"."药物剂型代码" IS '药物剂型类别(如颗粒剂、片剂、丸剂、胶囊剂等)在特定编码体系中的代码';
COMMENT ON COLUMN "治疗用药记录"."用药频次名称" IS '单位时间内药物使用频次类别的标准名称，如每天两次、每周两次、睡前一次等';
COMMENT ON COLUMN "治疗用药记录"."用药频次代码" IS '单位时间内药物使用的频次类别(如每天两次、每周两次、睡前一次等)在特定编码体系中的代码';
COMMENT ON COLUMN "治疗用药记录"."中药类别代码" IS '中药使用类别(如未使用、中成药、中草药、其他中药)在特定编码体系中的代码';
COMMENT ON COLUMN "治疗用药记录"."药品用法" IS '对治疗疾病所用药物的具体服用方法的详细描述';
COMMENT ON COLUMN "治疗用药记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "治疗用药记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "治疗用药记录"."药物名称" IS '药品在特定编码体系中的名称。对于医疗机构制剂，此处填写医疗机构制剂名称';
COMMENT ON COLUMN "治疗用药记录"."治疗记录流水号" IS '按照某一特定编码规则赋予治疗记录的唯一标识';
COMMENT ON COLUMN "治疗用药记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "治疗用药记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "治疗用药记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "治疗用药记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "治疗用药记录"."用药明细流水号" IS '按照某一特定编码规则赋予每条用药明细的顺序号';
COMMENT ON COLUMN "治疗用药记录"."用药途径代码" IS '药物使用途径(如口服、静滴、喷喉等)在特定编码体系中的代码';
COMMENT ON TABLE "治疗用药记录" IS '患者用药明细记录';


CREATE TABLE IF NOT EXISTS "死亡病例讨论记录" (
"医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "死亡病例讨论记录流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "讨论时间" timestamp DEFAULT NULL,
 "讨论地点" varchar (50) DEFAULT NULL,
 "主持人姓名" varchar (50) DEFAULT NULL,
 "参加讨论人员名单" varchar (1000) DEFAULT NULL,
 "专业技术职务类别代码" varchar (20) DEFAULT NULL,
 "直接死亡原因代码" varchar (20) DEFAULT NULL,
 "直接死亡原因名称" varchar (512) DEFAULT NULL,
 "死亡诊断代码" varchar (64) DEFAULT NULL,
 "死亡诊断名称" varchar (512) DEFAULT NULL,
 "死亡讨论记录" text,
 "主持人总结意见" text,
 "主治医师工号" varchar (20) DEFAULT NULL,
 "主治医师姓名" varchar (50) DEFAULT NULL,
 "主任医师工号" varchar (20) DEFAULT NULL,
 "主任医师姓名" varchar (50) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "文本内容" text,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "死亡病例讨论记录"_"医疗机构代码"_"死亡病例讨论记录流水号"_PK PRIMARY KEY ("医疗机构代码",
 "死亡病例讨论记录流水号")
);
COMMENT ON COLUMN "死亡病例讨论记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡病例讨论记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡病例讨论记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "死亡病例讨论记录"."文本内容" IS '存入大文本的内容，最大不超过64K';
COMMENT ON COLUMN "死亡病例讨论记录"."签名时间" IS '医师姓名完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡病例讨论记录"."主任医师姓名" IS '主任医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡病例讨论记录"."主任医师工号" IS '主任医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "死亡病例讨论记录"."主治医师姓名" IS '所在科室的具有主治医师专业技术职务资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡病例讨论记录"."主治医师工号" IS '所在科室的具有主治医师的工号';
COMMENT ON COLUMN "死亡病例讨论记录"."主持人总结意见" IS '主持人就讨论过程总结意见的详细描述';
COMMENT ON COLUMN "死亡病例讨论记录"."死亡讨论记录" IS '死亡患者讨论的具体讨论意见的详细描述';
COMMENT ON COLUMN "死亡病例讨论记录"."死亡诊断名称" IS '导致患者死亡的西医疾病诊断标准名称,如果有多个疾病诊断,这里指与其他疾病有因果关系的,并因其发生发展引起其他疾病,最终导致死亡的一系列疾病诊断中最初确定的疾病诊断名称';
COMMENT ON COLUMN "死亡病例讨论记录"."死亡诊断代码" IS '按照平台内编码规则赋予死亡诊断疾病的唯一标识';
COMMENT ON COLUMN "死亡病例讨论记录"."直接死亡原因名称" IS '直接导致患者死亡的最终疾病或原因的名称';
COMMENT ON COLUMN "死亡病例讨论记录"."直接死亡原因代码" IS '按照平台编码规则赋予直接死因的唯一标识';
COMMENT ON COLUMN "死亡病例讨论记录"."专业技术职务类别代码" IS '医护人员专业技术职务分类（如正高、副高、中级、助理等）在特定编码体系中的代码';
COMMENT ON COLUMN "死亡病例讨论记录"."参加讨论人员名单" IS '参与讨论人员的姓名列表，以”，“分隔';
COMMENT ON COLUMN "死亡病例讨论记录"."主持人姓名" IS '讨论记录主持人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡病例讨论记录"."讨论地点" IS '诊疗相关讨论所在地点的具体描述';
COMMENT ON COLUMN "死亡病例讨论记录"."讨论时间" IS '讨论记录开始时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡病例讨论记录"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "死亡病例讨论记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "死亡病例讨论记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "死亡病例讨论记录"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "死亡病例讨论记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "死亡病例讨论记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "死亡病例讨论记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡病例讨论记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "死亡病例讨论记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "死亡病例讨论记录"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "死亡病例讨论记录"."科室名称" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "死亡病例讨论记录"."科室代码" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "死亡病例讨论记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "死亡病例讨论记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "死亡病例讨论记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "死亡病例讨论记录"."住院次数" IS '办理完整住院治疗手续的次数';
COMMENT ON COLUMN "死亡病例讨论记录"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "死亡病例讨论记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "死亡病例讨论记录"."死亡病例讨论记录流水号" IS '按照某一特性编码规则赋予死亡病历讨论记录的唯一标识';
COMMENT ON COLUMN "死亡病例讨论记录"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "死亡病例讨论记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON TABLE "死亡病例讨论记录" IS '针对死亡患者的死亡原因、死亡诊断、诊疗过程等进行讨论的结论性记录';


CREATE TABLE IF NOT EXISTS "检验明细表" (
"临床项目名称" varchar (200) DEFAULT NULL,
 "省互认项目名称" varchar (200) DEFAULT NULL,
 "检验指标项目代码" varchar (32) DEFAULT NULL,
 "检验指标项目名称" varchar (200) DEFAULT NULL,
 "收费项目代码" varchar (64) DEFAULT NULL,
 "收费项目名称" varchar (100) DEFAULT NULL,
 "LOINC代码" varchar (10) DEFAULT NULL,
 "打印序号" decimal (10,
 2) DEFAULT NULL,
 "检验结果类型代码" varchar (1) DEFAULT NULL,
 "检验结果描述" varchar (200) DEFAULT NULL,
 "检验定性结果" varchar (200) DEFAULT NULL,
 "检验定量结果" varchar (10) DEFAULT NULL,
 "检验计量单位" varchar (20) DEFAULT NULL,
 "检验结果异常代码" varchar (2) DEFAULT NULL,
 "设备类别代码" varchar (20) DEFAULT NULL,
 "检验方法" varchar (32) DEFAULT NULL,
 "仪器编号" varchar (20) DEFAULT NULL,
 "仪器名称" varchar (100) DEFAULT NULL,
 "参考值范围" varchar (50) DEFAULT NULL,
 "参考值上限" decimal (10,
 2) DEFAULT NULL,
 "参考值下限" decimal (10,
 2) DEFAULT NULL,
 "检验报告结果" varchar (100) DEFAULT NULL,
 "备注" varchar (200) DEFAULT NULL,
 "医嘱单号" varchar (32) DEFAULT NULL,
 "医嘱明细流水号" varchar (32) DEFAULT NULL,
 "报告时间" timestamp DEFAULT NULL,
 "检测人工号" varchar (20) DEFAULT NULL,
 "检测人姓名" varchar (50) DEFAULT NULL,
 "审核人工号" varchar (20) DEFAULT NULL,
 "省互认项目代码" varchar (32) DEFAULT NULL,
 "检查结果代码" varchar (1) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "审核人姓名" varchar (50) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "检验明细流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "检验报告流水号" varchar (64) DEFAULT NULL,
 "临床项目代码" varchar (64) DEFAULT NULL,
 CONSTRAINT "检验明细表"_"医疗机构代码"_"检验明细流水号"_PK PRIMARY KEY ("医疗机构代码",
 "检验明细流水号")
);
COMMENT ON COLUMN "检验明细表"."临床项目代码" IS '医院内部使用检验套餐编码，如果是小项目则传具体检验项目编码。互认项目必填';
COMMENT ON COLUMN "检验明细表"."检验报告流水号" IS '按照某一特定编码规则赋予检验报告记录的唯一标识，在同一家医院内唯一';
COMMENT ON COLUMN "检验明细表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "检验明细表"."检验明细流水号" IS '按照某一特定编码规则赋予每条检验指标结果的顺序号，是医院内部唯一标示此检验指标记录的一个序号';
COMMENT ON COLUMN "检验明细表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "检验明细表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "检验明细表"."审核人姓名" IS '审核报告的医师签署在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "检验明细表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "检验明细表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "检验明细表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "检验明细表"."检查结果代码" IS '受检者检验结果(如正常、异常、不详等)在特定编码体系中的代码';
COMMENT ON COLUMN "检验明细表"."省互认项目代码" IS '检验项目在省互认编码体系中对应的代码，本处指省组合项目编码，互认项目必填';
COMMENT ON COLUMN "检验明细表"."审核人工号" IS '审核医师在在原始特定编码体系中的编号';
COMMENT ON COLUMN "检验明细表"."检测人姓名" IS '检测人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "检验明细表"."检测人工号" IS '检测人在原始特定编码体系中的编号';
COMMENT ON COLUMN "检验明细表"."报告时间" IS '报告生成当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "检验明细表"."医嘱明细流水号" IS '医疗记录中用于标识医嘱明细项目的唯一编号';
COMMENT ON COLUMN "检验明细表"."医嘱单号" IS '医疗记录系统中用于唯一标识医嘱单的本地标识符';
COMMENT ON COLUMN "检验明细表"."备注" IS '关于检验报告结果的其他说明';
COMMENT ON COLUMN "检验明细表"."检验报告结果" IS '检验报告结果的描述';
COMMENT ON COLUMN "检验明细表"."参考值下限" IS '定量结果值的正常参考下限值';
COMMENT ON COLUMN "检验明细表"."参考值上限" IS '定量结果值的正常参考上限值';
COMMENT ON COLUMN "检验明细表"."参考值范围" IS '对结果正常参考值范围的详细描述。有些性激素的参考值和性别以及所处周期相关，无法拆解';
COMMENT ON COLUMN "检验明细表"."仪器名称" IS '检验设备的名称';
COMMENT ON COLUMN "检验明细表"."仪器编号" IS '检验设备在特定编码体系中的编号';
COMMENT ON COLUMN "检验明细表"."检验方法" IS '所使用检验方法的详细描述，如化学法';
COMMENT ON COLUMN "检验明细表"."设备类别代码" IS '检验设备类别的编号';
COMMENT ON COLUMN "检验明细表"."检验结果异常代码" IS '受检者检验结果提示的类型(如正常、无法识别的异常、异常偏高、异常偏低等)在特定编码体系中的代码';
COMMENT ON COLUMN "检验明细表"."检验计量单位" IS '检验定量结果计量单位的名称';
COMMENT ON COLUMN "检验明细表"."检验定量结果" IS '受检者检验细项的定量结果的详细描述，如5.6或<0.35等。检验结果类型为1时，该字段必填';
COMMENT ON COLUMN "检验明细表"."检验定性结果" IS '受检者检验细项的定性结果，如阴性、阳性等。检验结果类型为2或3时，该字段必填';
COMMENT ON COLUMN "检验明细表"."检验结果描述" IS '对本人实验室检验结果的详细描述，包括定量结果和定性结果。对于细菌培养结果描述，如无细菌生长，则填写未发现致病菌；否则，则填写细菌名称';
COMMENT ON COLUMN "检验明细表"."检验结果类型代码" IS '检验结果的数据类型(如数值型、阴阳型、文本型)在特定编码体系中的代码';
COMMENT ON COLUMN "检验明细表"."打印序号" IS '同一份检验报告中多项检验指标项目结果的排序号';
COMMENT ON COLUMN "检验明细表"."LOINC代码" IS '检验项目在LOINC编码体系中的唯一标识符';
COMMENT ON COLUMN "检验明细表"."收费项目名称" IS '物价收费项目在机构内编码体系中的名称';
COMMENT ON COLUMN "检验明细表"."收费项目代码" IS '物价收费项目在机构内编码体系中的代码';
COMMENT ON COLUMN "检验明细表"."检验指标项目名称" IS '检验明细项目名称。医院可按实际情况填写，如红细胞计数';
COMMENT ON COLUMN "检验明细表"."检验指标项目代码" IS '检验明细项目代码，可填写通用的英文简称（如：“红细胞计数”的英文缩写为RBC）也可以是各医院系统里的唯一编码';
COMMENT ON COLUMN "检验明细表"."省互认项目名称" IS '检验项目在省互认编码体系中对应的名称，本处指省组合项目名称，互认项目必填';
COMMENT ON COLUMN "检验明细表"."临床项目名称" IS '医院内部使用检验套餐名称，如果是小项目则传具体检验项目编码。互认项目必填';
COMMENT ON TABLE "检验明细表" IS '常规检验，检验指标结果信息';


CREATE TABLE IF NOT EXISTS "检查报告影像索引表" (
"检查报告流水号" varchar (36) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "影像UID" varchar (200) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "报告时间" timestamp DEFAULT NULL,
 "存储地址" varchar (200) NOT NULL,
 "序列UID" varchar (200) DEFAULT NULL,
 CONSTRAINT "检查报告影像索引表"_"医疗机构代码"_"存储地址"_PK PRIMARY KEY ("医疗机构代码",
 "存储地址")
);
COMMENT ON COLUMN "检查报告影像索引表"."序列UID" IS '影像序列的唯一标识';
COMMENT ON COLUMN "检查报告影像索引表"."存储地址" IS '影像图片的具体存储地址';
COMMENT ON COLUMN "检查报告影像索引表"."报告时间" IS '报告生成当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "检查报告影像索引表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "检查报告影像索引表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "检查报告影像索引表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "检查报告影像索引表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "检查报告影像索引表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "检查报告影像索引表"."影像UID" IS '影像图片的唯一标识';
COMMENT ON COLUMN "检查报告影像索引表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "检查报告影像索引表"."检查报告流水号" IS '按照某一特定编码规则赋予检查报告记录的唯一标识';
COMMENT ON TABLE "检查报告影像索引表" IS '检查报告的影像UID地址';


CREATE TABLE IF NOT EXISTS "术前讨论" (
"年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "术前讨论流水号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "拟实施手术及操作时间" timestamp DEFAULT NULL,
 "拟实施麻醉方法代码" varchar (20) DEFAULT NULL,
 "拟实施麻醉方法名称" varchar (1000) DEFAULT NULL,
 "手术要点" varchar (200) DEFAULT NULL,
 "术前准备" text,
 "手术指征" varchar (500) DEFAULT NULL,
 "手术方案" text,
 "注意事项" text,
 "讨论意见" text,
 "讨论结论" text,
 "手术者工号" varchar (20) DEFAULT NULL,
 "手术者姓名" varchar (50) DEFAULT NULL,
 "麻醉医师工号" varchar (20) DEFAULT NULL,
 "麻醉医师姓名" varchar (50) DEFAULT NULL,
 "医师工号" varchar (20) DEFAULT NULL,
 "医师姓名" varchar (50) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "专业技术职务类别名称" varchar (50) DEFAULT NULL,
 "专业技术职务类别代码" varchar (50) DEFAULT NULL,
 "参加讨论人员名单" varchar (1000) DEFAULT NULL,
 "主持人姓名" varchar (50) DEFAULT NULL,
 "讨论地点" varchar (50) DEFAULT NULL,
 "讨论时间" timestamp DEFAULT NULL,
 "术前诊断代码" varchar (64) DEFAULT NULL,
 "术前诊断名称" varchar (100) DEFAULT NULL,
 "电子申请单编号" varchar (100) DEFAULT NULL,
 "拟实施手术及操作代码" varchar (50) DEFAULT NULL,
 "拟实施手术及操作名称" varchar (80) DEFAULT NULL,
 "拟实施手术部位名称" varchar (50) DEFAULT NULL,
 CONSTRAINT "术前讨论"_"术前讨论流水号"_"医疗机构代码"_PK PRIMARY KEY ("术前讨论流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "术前讨论"."拟实施手术部位名称" IS '拟实施手术的人体目标部位的名称，如双侧鼻孔、臀部、左臂、右眼等';
COMMENT ON COLUMN "术前讨论"."拟实施手术及操作名称" IS '拟实施的手术及操作在特定编码体系中的名称';
COMMENT ON COLUMN "术前讨论"."拟实施手术及操作代码" IS '按照平台特定编码规则赋予拟实施的手术及操作的唯一标识';
COMMENT ON COLUMN "术前讨论"."电子申请单编号" IS '按照某一特定编码规则赋予电子申请单的顺序号';
COMMENT ON COLUMN "术前讨论"."术前诊断名称" IS '术前诊断在机构内编码规则中对应的名称，如有多个用,隔开';
COMMENT ON COLUMN "术前讨论"."术前诊断代码" IS '按照平台编码规则赋予术前诊断的唯一标识';
COMMENT ON COLUMN "术前讨论"."讨论时间" IS '讨论记录开始时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "术前讨论"."讨论地点" IS '诊疗相关讨论所在地点的具体描述';
COMMENT ON COLUMN "术前讨论"."主持人姓名" IS '讨论记录主持人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "术前讨论"."参加讨论人员名单" IS '参与讨论人员的姓名列表，以”，“分隔';
COMMENT ON COLUMN "术前讨论"."专业技术职务类别代码" IS '医护人员专业技术职务分类（如正高、副高、中级、助理等）在特定编码体系中的代码';
COMMENT ON COLUMN "术前讨论"."专业技术职务类别名称" IS '医护人员专业技术职务分类（如正高、副高、中级、助理等）';
COMMENT ON COLUMN "术前讨论"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "术前讨论"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "术前讨论"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "术前讨论"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "术前讨论"."签名时间" IS '医师姓名完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "术前讨论"."医师姓名" IS '记医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "术前讨论"."医师工号" IS '医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "术前讨论"."麻醉医师姓名" IS '对患者实施麻醉的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "术前讨论"."麻醉医师工号" IS '麻醉医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "术前讨论"."手术者姓名" IS '实施手术的主要执行人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "术前讨论"."手术者工号" IS '实施手术的主要执行人员在原始特定编码体系中的编号';
COMMENT ON COLUMN "术前讨论"."讨论结论" IS '主持人就讨论过程总结意见的详细描述';
COMMENT ON COLUMN "术前讨论"."讨论意见" IS '讨论过程中的具体讨论意见的详细描述';
COMMENT ON COLUMN "术前讨论"."注意事项" IS '对可能出现问题及采取相应措施的描述';
COMMENT ON COLUMN "术前讨论"."手术方案" IS '手术方案的详细描述';
COMMENT ON COLUMN "术前讨论"."手术指征" IS '患者具备的、适宜实施手术的主要症状和体征描述';
COMMENT ON COLUMN "术前讨论"."术前准备" IS '手术前准备工作的详细描述';
COMMENT ON COLUMN "术前讨论"."手术要点" IS '手术要点的详细描述';
COMMENT ON COLUMN "术前讨论"."拟实施麻醉方法名称" IS '拟为患者进行手术、操作时使用的麻醉方法';
COMMENT ON COLUMN "术前讨论"."拟实施麻醉方法代码" IS '拟为患者进行手术、操作时使用的麻醉方法在特定编码体系中的代码';
COMMENT ON COLUMN "术前讨论"."拟实施手术及操作时间" IS '拟对患者开始手术操作时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "术前讨论"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "术前讨论"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "术前讨论"."术前讨论流水号" IS '按照某一特性编码规则赋予术前讨论记录的唯一标识';
COMMENT ON COLUMN "术前讨论"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "术前讨论"."住院次数" IS '办理完整住院治疗手续的次数';
COMMENT ON COLUMN "术前讨论"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "术前讨论"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "术前讨论"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "术前讨论"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "术前讨论"."科室代码" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "术前讨论"."科室名称" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "术前讨论"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "术前讨论"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "术前讨论"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "术前讨论"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "术前讨论"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "术前讨论"."性别名称" IS '一般指患者医学生理性别，指男性或女性';
COMMENT ON COLUMN "术前讨论"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "术前讨论"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "术前讨论"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "术前讨论"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "术前讨论"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON TABLE "术前讨论" IS '手术前针对患者拟实施手术、手术要点、手术指征 注意事项、讨论结果的记录';


CREATE TABLE IF NOT EXISTS "日常病程记录" (
"卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "医师姓名" varchar (50) DEFAULT NULL,
 "医师工号" varchar (20) DEFAULT NULL,
 "文本内容" text,
 "专业技术职务类别名称" varchar (50) DEFAULT NULL,
 "专业技术职务类别代码" varchar (50) DEFAULT NULL,
 "中药用药方法" varchar (100) DEFAULT NULL,
 "中药煎煮方法" varchar (100) DEFAULT NULL,
 "辨证论治详细描述" text,
 "中医“四诊”观察结果" text,
 "医嘱内容" text,
 "住院病程" text,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "日常病程记录流水号" varchar (64) NOT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "记录时间" timestamp DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 CONSTRAINT "日常病程记录"_"医疗机构代码"_"日常病程记录流水号"_PK PRIMARY KEY ("医疗机构代码",
 "日常病程记录流水号")
);
COMMENT ON COLUMN "日常病程记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "日常病程记录"."科室代码" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "日常病程记录"."科室名称" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "日常病程记录"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "日常病程记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "日常病程记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "日常病程记录"."记录时间" IS '完成首次病程记录的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "日常病程记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "日常病程记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "日常病程记录"."性别名称" IS '一般指患者医学生理性别，指男性或女性';
COMMENT ON COLUMN "日常病程记录"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "日常病程记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "日常病程记录"."日常病程记录流水号" IS '按照一定编码规则赋予日常病程记录的唯一标识';
COMMENT ON COLUMN "日常病程记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "日常病程记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "日常病程记录"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名';
COMMENT ON COLUMN "日常病程记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "日常病程记录"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "日常病程记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "日常病程记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "日常病程记录"."住院病程" IS '住院病历中病程记录内容的详细描述';
COMMENT ON COLUMN "日常病程记录"."医嘱内容" IS '医嘱内容的详细描述';
COMMENT ON COLUMN "日常病程记录"."中医“四诊”观察结果" IS '中医“四诊”观察结果的详细描述，包括望、闻、问、切四诊内容';
COMMENT ON COLUMN "日常病程记录"."辨证论治详细描述" IS '对辨证分型的名称、主要依据和采用的治则治法的详细描述';
COMMENT ON COLUMN "日常病程记录"."中药煎煮方法" IS '中药饮片煎煮方法描述，如水煎等';
COMMENT ON COLUMN "日常病程记录"."中药用药方法" IS '中药的用药方法的描述，如bid 煎服，先煎、后下等';
COMMENT ON COLUMN "日常病程记录"."专业技术职务类别代码" IS '医护人员专业技术职务分类（如正高、副高、中级、助理等）在特定编码体系中的代码';
COMMENT ON COLUMN "日常病程记录"."专业技术职务类别名称" IS '医护人员专业技术职务分类（如正高、副高、中级、助理等）';
COMMENT ON COLUMN "日常病程记录"."文本内容" IS '存入大文本的内容，最大不超过64K';
COMMENT ON COLUMN "日常病程记录"."医师工号" IS '医师的工号';
COMMENT ON COLUMN "日常病程记录"."医师姓名" IS '医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "日常病程记录"."签名时间" IS '医师姓名完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "日常病程记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "日常病程记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "日常病程记录"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "日常病程记录"."住院次数" IS '办理完整住院治疗手续的次数';
COMMENT ON COLUMN "日常病程记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "日常病程记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON TABLE "日常病程记录" IS '患者入院诊疗过程中的中医诊治结果、用药方法、医嘱相关内容的记录';


CREATE TABLE IF NOT EXISTS "新冠核酸或抗原阳性监管统计指标明细表" (
"新冠病毒核酸或抗原阳性—NICU基础病重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—其他专科ICU危重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性-其他专科ICU基础病重症数" decimal (4,
 0) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—ICU危重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—ICU新冠重症总数" decimal (4,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "数据日期" date NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "新冠病毒核酸或抗原阳性—其他专科ICU新冠重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—ICU基础病重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—综合ICU新冠重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—综合ICU危重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—综合ICU基础病重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—呼吸ICU新冠重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—呼吸ICU危重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—呼吸ICU基础病重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—PICU新冠重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—PICU危重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—PICU基础病重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—NICU新冠重症数" decimal (4,
 0) DEFAULT NULL,
 "新冠病毒核酸或抗原阳性—NICU危重症数" decimal (4,
 0) DEFAULT NULL,
 CONSTRAINT "新冠核酸或抗原阳性监管统计指标明细表"_"数据日期"_"医疗机构代码"_PK PRIMARY KEY ("数据日期",
 "医疗机构代码")
);
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—NICU危重症数" IS '截止统计当日的新冠核酸或抗原阳性NICU患者中危重患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—NICU新冠重症数" IS '截止统计当日的新冠核酸或抗原阳性NICU患者中重症患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—PICU基础病重症数" IS '截止统计当日的新冠核酸或抗原阳性PICU患者中基础病重患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—PICU危重症数" IS '截止统计当日的新冠核酸或抗原阳性PICU患者中危重患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—PICU新冠重症数" IS '截止统计当日的新冠核酸或抗原阳性PICU患者中重症患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—呼吸ICU基础病重症数" IS '截止统计当日的新冠核酸或抗原阳性呼吸ICU患者中基础病重患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—呼吸ICU危重症数" IS '截止统计当日的新冠核酸或抗原阳性呼吸ICU患者中危重患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—呼吸ICU新冠重症数" IS '截止统计当日的新冠核酸或抗原阳性呼吸ICU患者中重症患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—综合ICU基础病重症数" IS '截止统计当日的新冠核酸或抗原阳性综合ICU患者中基础病重患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—综合ICU危重症数" IS '截止统计当日的新冠核酸或抗原阳性综合ICU患者中危重患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—综合ICU新冠重症数" IS '截止统计当日的新冠核酸或抗原阳性综合ICU患者中重症患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—ICU基础病重症数" IS '截止统计当日的新冠核酸或抗原阳性ICU患者中基础病重患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—其他专科ICU新冠重症数" IS '截止统计当日的新冠核酸或抗原阳性其他专科ICU患者中重症患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."数据日期" IS '数据统计当日的公元纪年日期';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—ICU新冠重症总数" IS '截止统计当日的新冠核酸或抗原阳性ICU患者中重症患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—ICU危重症数" IS '截止统计当日的新冠核酸或抗原阳性ICU患者中危重患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性-其他专科ICU基础病重症数" IS '截止统计当日的新冠核酸或抗原阳性其他专科ICU患者中基础病重患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—其他专科ICU危重症数" IS '截止统计当日的新冠核酸或抗原阳性其他专科ICU患者中危重患者总人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计指标明细表"."新冠病毒核酸或抗原阳性—NICU基础病重症数" IS '截止统计当日的新冠核酸或抗原阳性NICU患者中基础病重患者总人数';
COMMENT ON TABLE "新冠核酸或抗原阳性监管统计指标明细表" IS '按日统计医院不同种类新冠核酸或抗原阳性患者数，如新冠核酸或抗原阳性的ICU新冠重症总数、ICU危重症数等';


CREATE TABLE IF NOT EXISTS "数据量记录汇总" (
"业务结束时间" timestamp NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "应传总数量" decimal (10,
 0) DEFAULT NULL,
 "明细数据上传标志" varchar (1) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "平台表名" varchar (64) NOT NULL,
 "业务开始时间" timestamp NOT NULL,
 CONSTRAINT "数据量记录汇总"_"业务结束时间"_"医疗机构代码"_"平台表名"_"业务开始时间"_PK PRIMARY KEY ("业务结束时间",
 "医疗机构代码",
 "平台表名",
 "业务开始时间")
);
COMMENT ON COLUMN "数据量记录汇总"."业务开始时间" IS '本张表的业务系统业务系统交易的开始时间';
COMMENT ON COLUMN "数据量记录汇总"."平台表名" IS '平台的前置机库中需要上传数据的表名，大写';
COMMENT ON COLUMN "数据量记录汇总"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "数据量记录汇总"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "数据量记录汇总"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "数据量记录汇总"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "数据量记录汇总"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "数据量记录汇总"."明细数据上传标志" IS '明细数据上传情况标志';
COMMENT ON COLUMN "数据量记录汇总"."应传总数量" IS '业务系统某张表在开始和结束时间段内应传数据的总数量，如his和社区系统';
COMMENT ON COLUMN "数据量记录汇总"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "数据量记录汇总"."业务结束时间" IS '本张表的业务系统业务系统交易的结束时间，业务开始日期和结束日期必须是同一天';
COMMENT ON TABLE "数据量记录汇总" IS '统计业务系统(如his，医技，社区系统)应传给平台的各张表的数据汇总，以便平台进行明细数据的比对监控';


CREATE TABLE IF NOT EXISTS "数据字典对应信息" (
"记录编号" varchar (48) NOT NULL,
 "代码类别" varchar (30) DEFAULT NULL,
 "代码名称" varchar (150) DEFAULT NULL,
 "类别名称" varchar (75) DEFAULT NULL,
 "代码值" varchar (45) DEFAULT NULL,
 "经办人" varchar (75) DEFAULT NULL,
 "创建时间" timestamp DEFAULT NULL,
 "创建人" varchar (75) DEFAULT NULL,
 "统筹区代码" varchar (20) DEFAULT NULL,
 "有效标志" varchar (1) DEFAULT NULL,
 "备注" varchar (1000) DEFAULT NULL,
 "参数值域说明" varchar (150) DEFAULT NULL,
 "代码可维护标志" varchar (1) DEFAULT NULL,
 "经办时间" timestamp DEFAULT NULL,
 "经办机构代码" varchar (24) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "数据字典对应信息"_"记录编号"_PK PRIMARY KEY ("记录编号")
);
COMMENT ON COLUMN "数据字典对应信息"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "数据字典对应信息"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "数据字典对应信息"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "数据字典对应信息"."经办机构代码" IS '按照某一特定编码规则赋予经办机构的唯一标识号';
COMMENT ON COLUMN "数据字典对应信息"."经办时间" IS '经办完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "数据字典对应信息"."代码可维护标志" IS '标识代码是否可维护的标志';
COMMENT ON COLUMN "数据字典对应信息"."参数值域说明" IS '参数值域的详细说明';
COMMENT ON COLUMN "数据字典对应信息"."备注" IS '其他重要内容的详细描述';
COMMENT ON COLUMN "数据字典对应信息"."有效标志" IS '标识数据是否有效的标志';
COMMENT ON COLUMN "数据字典对应信息"."统筹区代码" IS '统筹区的特定编码';
COMMENT ON COLUMN "数据字典对应信息"."创建人" IS '创建人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "数据字典对应信息"."创建时间" IS '创建完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "数据字典对应信息"."经办人" IS '经办人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "数据字典对应信息"."代码值" IS '代码的具体值';
COMMENT ON COLUMN "数据字典对应信息"."类别名称" IS '数据字典代码对应的类别在特定编码体系中的名称';
COMMENT ON COLUMN "数据字典对应信息"."代码名称" IS '代码值对应的名称';
COMMENT ON COLUMN "数据字典对应信息"."代码类别" IS '数据字典代码对应的类别在特定编码体系中的代码';
COMMENT ON COLUMN "数据字典对应信息"."记录编号" IS '数据字典记录对应的唯一标识';
COMMENT ON TABLE "数据字典对应信息" IS '数据字典表，包含代码值、代码名称、代码类别等';


CREATE TABLE IF NOT EXISTS "收费项目目录" (
"收费项目等级" decimal (10,
 2) DEFAULT NULL,
 "项目单价" decimal (10,
 2) DEFAULT NULL,
 "项目单位" varchar (64) DEFAULT NULL,
 "项目分类" varchar (12) DEFAULT NULL,
 "项目规格" varchar (32) DEFAULT NULL,
 "地方医疗服务项目代码" varchar (20) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "项目代码" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "项目名称" varchar (64) DEFAULT NULL,
 "平台项目代码" varchar (32) DEFAULT NULL,
 "医保项目代码" varchar (53) DEFAULT NULL,
 "全国医疗服务项目代码" varchar (20) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "高值耗材标志" varchar (1) DEFAULT NULL,
 "文字医嘱标志" varchar (1) DEFAULT NULL,
 "院内自制标志" varchar (1) DEFAULT NULL,
 "院内或地方新增项目标志" varchar (1) DEFAULT NULL,
 "医疗服务制定依据" varchar (5) DEFAULT NULL,
 "统计月份" varchar (2) DEFAULT NULL,
 "统计年份" varchar (4) DEFAULT NULL,
 "收费项目类别代码" varchar (2) DEFAULT NULL,
 "记录状态" varchar (1) DEFAULT NULL,
 "计价单位" varchar (75) DEFAULT NULL,
 "项目除外内容" varchar (500) DEFAULT NULL,
 "项目说明" varchar (500) DEFAULT NULL,
 "项目内涵" varchar (500) DEFAULT NULL,
 "适用范围" varchar (500) DEFAULT NULL,
 "生产地类别" varchar (32) DEFAULT NULL,
 "生产厂家名称" varchar (70) DEFAULT NULL,
 "检查部位名称" varchar (50) DEFAULT NULL,
 "检查部位代码" varchar (15) DEFAULT NULL,
 "限制适用性别" varchar (5) DEFAULT NULL,
 "最小医师等级" varchar (5) DEFAULT NULL,
 "最小医院等级" varchar (5) DEFAULT NULL,
 "招标价格" decimal (10,
 2) DEFAULT NULL,
 "最高限价" decimal (10,
 2) DEFAULT NULL,
 "自付比例" decimal (10,
 2) DEFAULT NULL,
 CONSTRAINT "收费项目目录"_"医疗机构代码"_"项目代码"_PK PRIMARY KEY ("医疗机构代码",
 "项目代码")
);
COMMENT ON COLUMN "收费项目目录"."自付比例" IS '乙类项目的自付比例，甲类0，丙类1';
COMMENT ON COLUMN "收费项目目录"."最高限价" IS '收费项目限制的最高收费价格，计量单位为人民币元';
COMMENT ON COLUMN "收费项目目录"."招标价格" IS '该收费项目的招标价格';
COMMENT ON COLUMN "收费项目目录"."最小医院等级" IS '具备使用该收费项目的最小医院等级';
COMMENT ON COLUMN "收费项目目录"."最小医师等级" IS '具备使用该收费项目的最小医师等级';
COMMENT ON COLUMN "收费项目目录"."限制适用性别" IS '该收费项目仅适用性别描述';
COMMENT ON COLUMN "收费项目目录"."检查部位代码" IS '检查部位在特定编码体系中的代码';
COMMENT ON COLUMN "收费项目目录"."检查部位名称" IS '检查部位在特定编码体系中的名称';
COMMENT ON COLUMN "收费项目目录"."生产厂家名称" IS '药品、耗材、器械等的生产企业在工商局注册、审批通过后的企业名称';
COMMENT ON COLUMN "收费项目目录"."生产地类别" IS '标识该收费项目是进口或者国产的类别名称';
COMMENT ON COLUMN "收费项目目录"."适用范围" IS '该收费项目的适用范围描述';
COMMENT ON COLUMN "收费项目目录"."项目内涵" IS '对收费项目特别包含内容的详细描述';
COMMENT ON COLUMN "收费项目目录"."项目说明" IS '收费项目的详细描述';
COMMENT ON COLUMN "收费项目目录"."项目除外内容" IS '对收费项目所不包含内容的详细描述';
COMMENT ON COLUMN "收费项目目录"."计价单位" IS '收费项目计价单位描述';
COMMENT ON COLUMN "收费项目目录"."记录状态" IS '标识记录是否正常使用的标志，其中：0：正常；1：停用';
COMMENT ON COLUMN "收费项目目录"."收费项目类别代码" IS '收费项目类别，其中：1：医用材料；9：其他';
COMMENT ON COLUMN "收费项目目录"."统计年份" IS '收费项目的统计年度';
COMMENT ON COLUMN "收费项目目录"."统计月份" IS '收费项目的统计月份';
COMMENT ON COLUMN "收费项目目录"."医疗服务制定依据" IS '制定医疗服务项目的收费规范的依据来源';
COMMENT ON COLUMN "收费项目目录"."院内或地方新增项目标志" IS '医疗项目为院内或地方新增项目的标志';
COMMENT ON COLUMN "收费项目目录"."院内自制标志" IS '标识医疗服务项目是否是院内自制的标志';
COMMENT ON COLUMN "收费项目目录"."文字医嘱标志" IS '标识医疗服务项目是否来自文字医嘱的标志';
COMMENT ON COLUMN "收费项目目录"."高值耗材标志" IS '标识医疗服务项目是否是高值耗材的标志';
COMMENT ON COLUMN "收费项目目录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "收费项目目录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "收费项目目录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "收费项目目录"."全国医疗服务项目代码" IS '全国医疗服务价格项目规范中，具体收费项目的统一编码';
COMMENT ON COLUMN "收费项目目录"."医保项目代码" IS '该收费项目在医保目录编码规则下的代码';
COMMENT ON COLUMN "收费项目目录"."平台项目代码" IS '平台中心收费项目代码，与平台中心代码对应的上的必填';
COMMENT ON COLUMN "收费项目目录"."项目名称" IS '医疗机构内收费项目名称';
COMMENT ON COLUMN "收费项目目录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "收费项目目录"."项目代码" IS '医疗机构内收费项目代码';
COMMENT ON COLUMN "收费项目目录"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "收费项目目录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "收费项目目录"."地方医疗服务项目代码" IS '地方医疗服务价格项目规范中，具体收费项目的统一编码';
COMMENT ON COLUMN "收费项目目录"."项目规格" IS '收费项目的规格描述，如药品规格、器械材料规格等';
COMMENT ON COLUMN "收费项目目录"."项目分类" IS '收费项目分类描述，如检查费、检验费、耗材费等';
COMMENT ON COLUMN "收费项目目录"."项目单位" IS '项目计价单位名称';
COMMENT ON COLUMN "收费项目目录"."项目单价" IS '按计价单位收取的医疗服务价格(元)保留2位小数';
COMMENT ON COLUMN "收费项目目录"."收费项目等级" IS '收费项目的医保等级代码';
COMMENT ON TABLE "收费项目目录" IS '医疗机构内检验、检查、材料等医疗服务项目的分类名称、代码、内涵等基本信息';


CREATE TABLE IF NOT EXISTS "护理计划记录" (
"护士工号" varchar (20) DEFAULT NULL,
 "护士姓名" varchar (50) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "护理评估记录流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "疾病诊断名称" varchar (512) DEFAULT NULL,
 "护理等级代码" varchar (1) DEFAULT NULL,
 "护理等级名称" varchar (20) DEFAULT NULL,
 "护理类型代码" varchar (1) DEFAULT NULL,
 "护理类型名称" varchar (20) DEFAULT NULL,
 "护理问题" text,
 "导管护理描述" text,
 "气管护理代码" varchar (3) DEFAULT NULL,
 "气管护理" varchar (100) DEFAULT NULL,
 "体位护理描述" varchar (30) DEFAULT NULL,
 "皮肤护理描述" varchar (50) DEFAULT NULL,
 "安全护理代码" varchar (1) DEFAULT NULL,
 "安全护理名称" varchar (100) DEFAULT NULL,
 "饮食指导代码" varchar (2) DEFAULT NULL,
 "饮食指导名称" varchar (100) DEFAULT NULL,
 "护理观察项目名称" varchar (200) DEFAULT NULL,
 "护理观察结果" text,
 "护理操作名称" varchar (100) DEFAULT NULL,
 "护理操作项目类目名称" varchar (100) DEFAULT NULL,
 "护理操作结果" text,
 CONSTRAINT "护理计划记录"_"医疗机构代码"_"护理评估记录流水号"_PK PRIMARY KEY ("医疗机构代码",
 "护理评估记录流水号")
);
COMMENT ON COLUMN "护理计划记录"."护理操作结果" IS '护理操作结果的详细描述';
COMMENT ON COLUMN "护理计划记录"."护理操作项目类目名称" IS '多个护理操作项目的名称';
COMMENT ON COLUMN "护理计划记录"."护理操作名称" IS '进行护理操作的具体名称';
COMMENT ON COLUMN "护理计划记录"."护理观察结果" IS '对护理观察项目结果的详细描述';
COMMENT ON COLUMN "护理计划记录"."护理观察项目名称" IS '护理观察项目的名称，如患者神志状态、饮食情况，皮肤情况、氧疗情况、排尿排便情况，流量、出量、人量等等，根据护理内容的不同选择不同的观察项目名称';
COMMENT ON COLUMN "护理计划记录"."饮食指导名称" IS '饮食指导类别(如普通饮食、软食、半流食、流食、禁食等)名称';
COMMENT ON COLUMN "护理计划记录"."饮食指导代码" IS '饮食指导类别(如普通饮食、软食、半流食、流食、禁食等)在特定编码体系中的代码';
COMMENT ON COLUMN "护理计划记录"."安全护理名称" IS '安全护理类别名称';
COMMENT ON COLUMN "护理计划记录"."安全护理代码" IS '安全护理类别在特定编码体系中的代码';
COMMENT ON COLUMN "护理计划记录"."皮肤护理描述" IS '对患者进行皮肤护理的描述';
COMMENT ON COLUMN "护理计划记录"."体位护理描述" IS '对患者进行体位护理的详细描述';
COMMENT ON COLUMN "护理计划记录"."气管护理" IS '气管护理类别名称';
COMMENT ON COLUMN "护理计划记录"."气管护理代码" IS '气管护理类别在特定编码体系中的代码';
COMMENT ON COLUMN "护理计划记录"."导管护理描述" IS '对患者进行导管护理的详细描述';
COMMENT ON COLUMN "护理计划记录"."护理问题" IS '患者人院后需要采取相应护理措施的问题描述';
COMMENT ON COLUMN "护理计划记录"."护理类型名称" IS '护理类型的分类名称';
COMMENT ON COLUMN "护理计划记录"."护理类型代码" IS '护理类型的分类在特定编码体系中的代码';
COMMENT ON COLUMN "护理计划记录"."护理等级名称" IS '护理级别的分类名称';
COMMENT ON COLUMN "护理计划记录"."护理等级代码" IS '护理级别的分类在特定编码体系中的代码';
COMMENT ON COLUMN "护理计划记录"."疾病诊断名称" IS '疾病诊断在特定编码体系中的名称';
COMMENT ON COLUMN "护理计划记录"."疾病诊断代码" IS '疾病诊断在特定编码体系中的代码';
COMMENT ON COLUMN "护理计划记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "护理计划记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "护理计划记录"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "护理计划记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "护理计划记录"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "护理计划记录"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "护理计划记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "护理计划记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "护理计划记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "护理计划记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "护理计划记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "护理计划记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "护理计划记录"."就诊次数" IS '患者在医院就诊的次数';
COMMENT ON COLUMN "护理计划记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "护理计划记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "护理计划记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "护理计划记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "护理计划记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "护理计划记录"."护理评估记录流水号" IS '按照某一特定编码规则赋予护理评估记录的顺序号';
COMMENT ON COLUMN "护理计划记录"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "护理计划记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "护理计划记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "护理计划记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "护理计划记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "护理计划记录"."签名时间" IS '护理护士在护理记录上完成签名的公元纪年和日期的完整描述';
COMMENT ON COLUMN "护理计划记录"."护士姓名" IS '护理护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "护理计划记录"."护士工号" IS '护理护士的工号';
COMMENT ON TABLE "护理计划记录" IS '制定护理计划时的各项评估指标结果';


CREATE TABLE IF NOT EXISTS "手术记录" (
"手术相关医院感染标志" varchar (1) DEFAULT NULL,
 "预防使用抗菌药物天数" decimal (4,
 0) DEFAULT NULL,
 "术中病理诊断代码" varchar (20) DEFAULT NULL,
 "术中病理诊断名称" varchar (64) DEFAULT NULL,
 "术后病理诊断代码" varchar (20) DEFAULT NULL,
 "术后病理诊断名称" varchar (64) DEFAULT NULL,
 "病理检查" varchar (500) DEFAULT NULL,
 "其他医学处置" text,
 "超出标准手术时间标志" varchar (1) DEFAULT NULL,
 "执行科室代码" varchar (20) DEFAULT NULL,
 "执行科室名称" varchar (100) DEFAULT NULL,
 "病案归档时间" timestamp DEFAULT NULL,
 "麻醉医师工号" varchar (20) DEFAULT NULL,
 "麻醉医师姓名" varchar (50) DEFAULT NULL,
 "麻醉医师签名时间" timestamp DEFAULT NULL,
 "手术医生工号" varchar (20) DEFAULT NULL,
 "手术医生姓名" varchar (50) DEFAULT NULL,
 "手术医生I助工号" varchar (20) DEFAULT NULL,
 "手术医生I助姓名" varchar (50) DEFAULT NULL,
 "手术医生II助工号" varchar (20) DEFAULT NULL,
 "预防使用抗菌药物标志" varchar (1) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "巡台护士姓名" varchar (50) DEFAULT NULL,
 "巡台护士工号" varchar (20) DEFAULT NULL,
 "器械护士姓名" varchar (50) DEFAULT NULL,
 "器械护士工号" varchar (20) DEFAULT NULL,
 "手术医生II助姓名" varchar (50) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "手术明细流水号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "就诊流水号" varchar (32) DEFAULT NULL,
 "手术组号" varchar (6) DEFAULT NULL,
 "手术序号" decimal (6,
 0) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" varchar (8) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "门(急)诊号" varchar (32) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "ABO血型名称" varchar (50) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "Rh血型名称" varchar (50) DEFAULT NULL,
 "电子申请单编号" varchar (100) DEFAULT NULL,
 "手术间编号" varchar (20) DEFAULT NULL,
 "手术起始时间" timestamp DEFAULT NULL,
 "手术结束时间" timestamp DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "手术类型代码" varchar (2) DEFAULT NULL,
 "术前诊断代码" varchar (100) DEFAULT NULL,
 "术前诊断名称" varchar (100) DEFAULT NULL,
 "术前发生院内感染标志" varchar (1) DEFAULT NULL,
 "术后诊断代码" varchar (64) DEFAULT NULL,
 "术后诊断名称" varchar (100) DEFAULT NULL,
 "手术操作代码" varchar (20) DEFAULT NULL,
 "手术操作名称" varchar (64) DEFAULT NULL,
 "手术级别代码" varchar (4) DEFAULT NULL,
 "手术级别名称" varchar (300) DEFAULT NULL,
 "手术部位代码" varchar (4) DEFAULT NULL,
 "手术体位代码" varchar (3) DEFAULT NULL,
 "手术过程描述" text,
 "手术史标志" varchar (1) DEFAULT NULL,
 "手术及操作方法" varchar (200) DEFAULT NULL,
 "手术及操作次数" decimal (3,
 0) DEFAULT NULL,
 "皮肤消毒描述" varchar (200) DEFAULT NULL,
 "手术切口描述" varchar (200) DEFAULT NULL,
 "手术切口类别代码" varchar (2) DEFAULT NULL,
 "手术切口类别名称" varchar (50) DEFAULT NULL,
 "切口愈合等级代码" varchar (3) DEFAULT NULL,
 "切口愈合等级名称" varchar (30) DEFAULT NULL,
 "手术合并症标志" varchar (1) DEFAULT NULL,
 "手术并发症标志" varchar (1) DEFAULT NULL,
 "手术并发症代码" varchar (20) DEFAULT NULL,
 "手术并发症名称" varchar (200) DEFAULT NULL,
 "介入物名称" varchar (100) DEFAULT NULL,
 "引流标志" varchar (1) DEFAULT NULL,
 "引流材料名称" varchar (200) DEFAULT NULL,
 "引流材料数目" varchar (200) DEFAULT NULL,
 "引流材料放置部位" varchar (50) DEFAULT NULL,
 "手术出血量" decimal (10,
 3) DEFAULT NULL,
 "输血反应标志" varchar (1) DEFAULT NULL,
 "输血(mL)" decimal (4,
 0) DEFAULT NULL,
 "输液量(mL)" decimal (5,
 0) DEFAULT NULL,
 "术前用药" varchar (100) DEFAULT NULL,
 "术中用药" varchar (100) DEFAULT NULL,
 "术后情况" varchar (500) DEFAULT NULL,
 "麻醉方式代码" varchar (4) DEFAULT NULL,
 "麻醉方式名称" varchar (50) DEFAULT NULL,
 "ASA分级代码" varchar (10) DEFAULT NULL,
 "ASA分级名称" varchar (50) DEFAULT NULL,
 "麻醉执行科室代码" varchar (20) DEFAULT NULL,
 "麻醉执行科室名称" varchar (100) DEFAULT NULL,
 "麻醉药物代码" varchar (50) DEFAULT NULL,
 "麻醉药物名称" varchar (250) DEFAULT NULL,
 "麻醉药物剂量" varchar (10) DEFAULT NULL,
 "剂量单位" varchar (20) DEFAULT NULL,
 "麻醉开始时间" timestamp DEFAULT NULL,
 "麻醉结束时间" timestamp DEFAULT NULL,
 "麻醉合并症标志" varchar (1) DEFAULT NULL,
 "麻醉合并症代码" varchar (10) DEFAULT NULL,
 "麻醉合并症名称" varchar (100) DEFAULT NULL,
 "麻醉合并症描述" varchar (200) DEFAULT NULL,
 "入复苏室时间" timestamp DEFAULT NULL,
 "出复苏室时间" timestamp DEFAULT NULL,
 "麻醉反应类别代码" varchar (1) DEFAULT NULL,
 "麻醉效果" varchar (100) DEFAULT NULL,
 "日间手术标志" varchar (1) DEFAULT NULL,
 "医源性手术标志" varchar (1) DEFAULT NULL,
 "本院手术标志" varchar (1) DEFAULT NULL,
 "主手术标志" varchar (1) DEFAULT NULL,
 "死亡标志" varchar (1) DEFAULT NULL,
 "择期手术标志" varchar (1) DEFAULT NULL,
 "择期取消手术标志" varchar (1) DEFAULT NULL,
 "非计划再次手术标志" varchar (1) DEFAULT NULL,
 "无菌手术标志" varchar (1) DEFAULT NULL,
 CONSTRAINT "手术记录"_"医疗机构代码"_"手术明细流水号"_PK PRIMARY KEY ("医疗机构代码",
 "手术明细流水号")
);
COMMENT ON COLUMN "手术记录"."无菌手术标志" IS '标识手术是否为无菌手术';
COMMENT ON COLUMN "手术记录"."非计划再次手术标志" IS '标识是否非计划再次手术的标志';
COMMENT ON COLUMN "手术记录"."择期取消手术标志" IS '标识患者是否择期取消手术的标志';
COMMENT ON COLUMN "手术记录"."择期手术标志" IS '标识是否择期手术的标志';
COMMENT ON COLUMN "手术记录"."死亡标志" IS '标识患者手术过程中是否死亡的标志';
COMMENT ON COLUMN "手术记录"."主手术标志" IS '标识是否为主手术';
COMMENT ON COLUMN "手术记录"."本院手术标志" IS '标识是否来源于本院手术';
COMMENT ON COLUMN "手术记录"."医源性手术标志" IS '标识是否由于医院原因导致该手术';
COMMENT ON COLUMN "手术记录"."日间手术标志" IS '标识是否为日间手术的标志';
COMMENT ON COLUMN "手术记录"."麻醉效果" IS '实施麻醉效果的描述';
COMMENT ON COLUMN "手术记录"."麻醉反应类别代码" IS '标识是否具有麻醉反应的情况，1:无麻醉，2：有反应，3:无反应';
COMMENT ON COLUMN "手术记录"."出复苏室时间" IS '患者离开复苏室的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术记录"."入复苏室时间" IS '患者进入复苏室的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术记录"."麻醉合并症描述" IS '麻醉合并症的详细描述';
COMMENT ON COLUMN "手术记录"."麻醉合并症名称" IS '患者麻醉合并症类型在特定编码体系中的名称';
COMMENT ON COLUMN "手术记录"."麻醉合并症代码" IS '患者麻醉合并症类型在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."麻醉合并症标志" IS '标识是否具有麻醉合并症的标志';
COMMENT ON COLUMN "手术记录"."麻醉结束时间" IS '麻醉结束时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术记录"."麻醉开始时间" IS '麻醉开始时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术记录"."剂量单位" IS '麻醉药物剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "手术记录"."麻醉药物剂量" IS '使用麻醉药物的总量';
COMMENT ON COLUMN "手术记录"."麻醉药物名称" IS '麻醉药物在特定编码体系中的名称';
COMMENT ON COLUMN "手术记录"."麻醉药物代码" IS '按照特定编码规则赋予麻醉药物的唯一标识';
COMMENT ON COLUMN "手术记录"."麻醉执行科室名称" IS '麻醉执行的机构内名称';
COMMENT ON COLUMN "手术记录"."麻醉执行科室代码" IS '按照机构内编码规则赋予麻醉执行的唯一标识';
COMMENT ON COLUMN "手术记录"."ASA分级名称" IS '根据美同麻醉师协会(ASA)制定的分级标准，对病人体质状况和对手术危险性进行评估分级的结果在特定编码体系中的名称';
COMMENT ON COLUMN "手术记录"."ASA分级代码" IS '根据美同麻醉师协会(ASA)制定的分级标准，对病人体质状况和对手术危险性进行评估分级的结果在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."麻醉方式名称" IS '为患者进行手术、操作时使用的麻醉方法在特定编码体系中的名称';
COMMENT ON COLUMN "手术记录"."麻醉方式代码" IS '为患者进行手术、操作时使用的麻醉方法在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."术后情况" IS '对手术后患者情况的详细描述';
COMMENT ON COLUMN "手术记录"."术中用药" IS '对患者术中用药情况的描述';
COMMENT ON COLUMN "手术记录"."术前用药" IS '对患者术前用药情况的描述';
COMMENT ON COLUMN "手术记录"."输液量(mL)" IS '总输液量，单位ml';
COMMENT ON COLUMN "手术记录"."输血(mL)" IS '输入红细胞、血小板、血浆、全血等的数量，剂量单位为mL';
COMMENT ON COLUMN "手术记录"."输血反应标志" IS '标志患者术中输血后是否发生了输血反应的标志';
COMMENT ON COLUMN "手术记录"."手术出血量" IS '手术过程的总出血量，单位ml';
COMMENT ON COLUMN "手术记录"."引流材料放置部位" IS '引流管放置在病人体内的具体位置的描述';
COMMENT ON COLUMN "手术记录"."引流材料数目" IS '对手术中引流材料数目的具体描述';
COMMENT ON COLUMN "手术记录"."引流材料名称" IS '对手术中引流材料名称的具体描述';
COMMENT ON COLUMN "手术记录"."引流标志" IS '标志术中是否有引流的标志';
COMMENT ON COLUMN "手术记录"."介入物名称" IS '实施手术操作时使用/放置的材料/药物的名称';
COMMENT ON COLUMN "手术记录"."手术并发症名称" IS '手术并发症在特定编码体系中的名称。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "手术记录"."手术并发症代码" IS '手术并发症在特定编码体系中的代码。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "手术记录"."手术并发症标志" IS '标识患者是否发生手术合并症';
COMMENT ON COLUMN "手术记录"."手术合并症标志" IS '标识患者是否发生手术合并症';
COMMENT ON COLUMN "手术记录"."切口愈合等级名称" IS '手术切口愈合类别)在特定编码体系中的名称，如甲、乙等';
COMMENT ON COLUMN "手术记录"."切口愈合等级代码" IS '手术切口愈合类别(如甲、乙、丙)在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."手术切口类别名称" IS '手术切口类别的分类(如0类切口、I类切口、II类切口、III类切口)在特定编码体系中的名称';
COMMENT ON COLUMN "手术记录"."手术切口类别代码" IS '手术切口类别的分类(如0类切口、I类切口、II类切口、III类切口)在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."手术切口描述" IS '对手术中皮肤切口情况的具体描述';
COMMENT ON COLUMN "手术记录"."皮肤消毒描述" IS '对手术中皮肤消毒情况的具体描述';
COMMENT ON COLUMN "手术记录"."手术及操作次数" IS '实施手术及操作的次数';
COMMENT ON COLUMN "手术记录"."手术及操作方法" IS '手术及操作方法的详细描述';
COMMENT ON COLUMN "手术记录"."手术史标志" IS '标识患者有无手术经历的标志';
COMMENT ON COLUMN "手术记录"."手术过程描述" IS '手术(操作)过程的详细描述';
COMMENT ON COLUMN "手术记录"."手术体位代码" IS '手术时为患者采取的体位(如仰卧位、俯卧位、左侧卧位、截石位、屈氏位等)在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."手术部位代码" IS '实施手术(操作)的人体部位(如双侧鼻孔、臀部、左臂、右眼等)在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."手术级别名称" IS '按照手术分级管理制度，根据风险性和难易程度不同划分的手术级别在特定编码体系中的名称，如一级手术、二级手术、三级手术、四级手术';
COMMENT ON COLUMN "手术记录"."手术级别代码" IS '按照手术分级管理制度，根据风险性和难易程度不同划分的手术级别(如一级手术、二级手术、三级手术、四级手术)在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."手术操作名称" IS '手术及操作在特定编码体系中的名称';
COMMENT ON COLUMN "手术记录"."手术操作代码" IS '手术及操作在特定编码体系中的唯一标识';
COMMENT ON COLUMN "手术记录"."术后诊断名称" IS '术后诊断在特定编码体系中的名称。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "手术记录"."术后诊断代码" IS '术后诊断在特定编码体系中的代码。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "手术记录"."术前发生院内感染标志" IS '标识患者手术前是否已经发生了院内感染的标志';
COMMENT ON COLUMN "手术记录"."术前诊断名称" IS '术前诊断在特定编码体系中的名称。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "手术记录"."术前诊断代码" IS '术前诊断在特定编码体系中的代码。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "手术记录"."手术类型代码" IS '根据病情急缓和手术时间划分的手术类型(如择期、加急手术、抢救手术等)在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."手术结束时间" IS '手术结束时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术记录"."手术起始时间" IS '手术开始时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术记录"."手术间编号" IS '患者实施手术所在的手术室编号';
COMMENT ON COLUMN "手术记录"."电子申请单编号" IS '按照某一特定编码规则赋予电子申请单的顺序号';
COMMENT ON COLUMN "手术记录"."Rh血型名称" IS '为患者实际输入的Rh血型的类别在特定编码体系中的名称';
COMMENT ON COLUMN "手术记录"."Rh血型代码" IS '为患者实际输入的Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."ABO血型名称" IS '为患者实际输入的ABO血型类别在特定编码体系中的名称';
COMMENT ON COLUMN "手术记录"."ABO血型代码" IS '为患者实际输入的ABO血型类别在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."病床号" IS '按照某一特定编码规则赋予患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "手术记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "手术记录"."病区名称" IS '患者当前所在病区在特定编码体系中的名称';
COMMENT ON COLUMN "手术记录"."就诊科室名称" IS '就诊科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在机构内编码体系中的名称';
COMMENT ON COLUMN "手术记录"."就诊科室代码" IS '就诊科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在机构内编码体系中的代码';
COMMENT ON COLUMN "手术记录"."门(急)诊号" IS '患者在门诊就诊时所获得的唯一标识号';
COMMENT ON COLUMN "手术记录"."住院次数" IS '此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "手术记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "手术记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "手术记录"."年龄(月)" IS '条件必填，年龄不足1周岁的实足年龄的月龄，以分数形式表示：分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天\r\n数。此时患者年龄(岁)填写值为“0”';
COMMENT ON COLUMN "手术记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，历法年龄，按照实足年龄的整数填写';
COMMENT ON COLUMN "手术记录"."性别代码" IS '患者生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "手术记录"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "手术记录"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "手术记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "手术记录"."手术序号" IS '同组手术的顺序号';
COMMENT ON COLUMN "手术记录"."手术组号" IS '用以标记同时做的手术';
COMMENT ON COLUMN "手术记录"."就诊流水号" IS '按照某一特定编码规则赋予就诊事件的唯一标识，同门急诊/住院关联';
COMMENT ON COLUMN "手术记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "手术记录"."手术明细流水号" IS '按照特定编码规则赋予输血记录的顺序号';
COMMENT ON COLUMN "手术记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "手术记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "手术记录"."手术医生II助姓名" IS '协助手术者完成手术及操作的第2助手在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "手术记录"."器械护士工号" IS '器械护士在原始特定编码体系中的编号';
COMMENT ON COLUMN "手术记录"."器械护士姓名" IS '器械护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "手术记录"."巡台护士工号" IS '巡台护士在原始特定编码体系中的编号';
COMMENT ON COLUMN "手术记录"."巡台护士姓名" IS '巡台护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "手术记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "手术记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术记录"."预防使用抗菌药物标志" IS '标识患者是否预防性使用了抗菌药物的标志';
COMMENT ON COLUMN "手术记录"."手术医生II助工号" IS '协助手术者完成手术及操作的第2助手在原始特定编码体系中的编号';
COMMENT ON COLUMN "手术记录"."手术医生I助姓名" IS '协助手术者完成手术及操作的第1助手在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "手术记录"."手术医生I助工号" IS '协助手术者完成手术及操作的第1助手在原始特定编码体系中的编号';
COMMENT ON COLUMN "手术记录"."手术医生姓名" IS '实施手术的主要执行人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "手术记录"."手术医生工号" IS '实施手术的主要执行人员在原始特定编码体系中的编号';
COMMENT ON COLUMN "手术记录"."麻醉医师签名时间" IS '麻醉医师完成签名的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术记录"."麻醉医师姓名" IS '对患者实施麻醉的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "手术记录"."麻醉医师工号" IS '麻醉医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "手术记录"."病案归档时间" IS '病案归档完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术记录"."执行科室名称" IS '手术执行科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在机构内编码体系中的名称';
COMMENT ON COLUMN "手术记录"."执行科室代码" IS '手术执行科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在机构内编码体系中的代码';
COMMENT ON COLUMN "手术记录"."超出标准手术时间标志" IS '标识患者的手术时间是否超出标准手术时间';
COMMENT ON COLUMN "手术记录"."其他医学处置" IS '其他医学处置的详细描述';
COMMENT ON COLUMN "手术记录"."病理检查" IS '病理检查的详细描述';
COMMENT ON COLUMN "手术记录"."术后病理诊断名称" IS '术后病理诊断在平台特定编码体系中的名称';
COMMENT ON COLUMN "手术记录"."术后病理诊断代码" IS '术后病理诊断在平台特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."术中病理诊断名称" IS '术中病理诊断在平台特定编码体系中的名称';
COMMENT ON COLUMN "手术记录"."术中病理诊断代码" IS '术中病理诊断在平台特定编码体系中的代码';
COMMENT ON COLUMN "手术记录"."预防使用抗菌药物天数" IS '预防使用抗菌药物的具体天数';
COMMENT ON COLUMN "手术记录"."手术相关医院感染标志" IS '标识手术相关的医院感染';
COMMENT ON TABLE "手术记录" IS '一个手术对应一条数据，若一次手术包含多个手术，则有多条数据';


CREATE TABLE IF NOT EXISTS "手术同意书" (
"科室代码" varchar (20) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "知情同意书编号" varchar (20) DEFAULT NULL,
 "门诊就诊流水号" varchar (32) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "手术同意书流水号" varchar (64) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "医师签名时间" timestamp DEFAULT NULL,
 "手术者姓名" varchar (50) DEFAULT NULL,
 "手术者工号" varchar (20) DEFAULT NULL,
 "经治医师姓名" varchar (50) DEFAULT NULL,
 "经治医师工号" varchar (20) DEFAULT NULL,
 "患者/法定代理人签名时间" timestamp DEFAULT NULL,
 "法定代理人与患者的关系名称" varchar (100) DEFAULT NULL,
 "法定代理人与患者的关系代码" varchar (1) DEFAULT NULL,
 "法定代理人姓名" varchar (50) DEFAULT NULL,
 "患者签名" varchar (50) DEFAULT NULL,
 "患者/法定代理人意见" text,
 "医疗机构意见" text,
 "替代方案" text,
 "手术后可能出现的意外及并发症" text,
 "手术中可能出现的意外及风险" varchar (200) DEFAULT NULL,
 "术前准备" text,
 "拟实施麻醉方法名称" varchar (1000) DEFAULT NULL,
 "拟实施麻醉方法代码" varchar (20) DEFAULT NULL,
 "手术方式" varchar (30) DEFAULT NULL,
 "手术禁忌症" varchar (100) DEFAULT NULL,
 "手术指征" varchar (500) DEFAULT NULL,
 "拟实施手术及操作时间" timestamp DEFAULT NULL,
 "拟实施手术及操作名称" varchar (80) DEFAULT NULL,
 "拟实施手术及操作代码" varchar (50) DEFAULT NULL,
 "术前诊断名称" varchar (100) DEFAULT NULL,
 "术前诊断代码" varchar (64) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 CONSTRAINT "手术同意书"_"医疗机构代码"_"手术同意书流水号"_PK PRIMARY KEY ("医疗机构代码",
 "手术同意书流水号")
);
COMMENT ON COLUMN "手术同意书"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "手术同意书"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "手术同意书"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "手术同意书"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "手术同意书"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "手术同意书"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "手术同意书"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "手术同意书"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "手术同意书"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "手术同意书"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "手术同意书"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "手术同意书"."术前诊断代码" IS '术前诊断在特定编码体系中的代码';
COMMENT ON COLUMN "手术同意书"."术前诊断名称" IS '术前诊断在特定编码体系中的名称。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "手术同意书"."拟实施手术及操作代码" IS '拟实施的手术及操作在特定编码体系中的代码';
COMMENT ON COLUMN "手术同意书"."拟实施手术及操作名称" IS '拟实施的手术及操作在特定编码体系中的名称';
COMMENT ON COLUMN "手术同意书"."拟实施手术及操作时间" IS '拟对患者开始手术操作时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术同意书"."手术指征" IS '患者具备的、适宜实施手术的主要症状和体征描述';
COMMENT ON COLUMN "手术同意书"."手术禁忌症" IS '拟实施手术的禁忌症的描述';
COMMENT ON COLUMN "手术同意书"."手术方式" IS '拟实施手术方式的详细描述';
COMMENT ON COLUMN "手术同意书"."拟实施麻醉方法代码" IS '拟为患者进行手术、操作时使用的麻醉方法在特定编码体系中的代码';
COMMENT ON COLUMN "手术同意书"."拟实施麻醉方法名称" IS '拟为患者进行手术、操作时使用的麻醉方法';
COMMENT ON COLUMN "手术同意书"."术前准备" IS '手术前准备工作的详细描述';
COMMENT ON COLUMN "手术同意书"."手术中可能出现的意外及风险" IS '手术中可能发生的意外情况以及风险描述';
COMMENT ON COLUMN "手术同意书"."手术后可能出现的意外及并发症" IS '手术中可能发生的意外情况及风险描述';
COMMENT ON COLUMN "手术同意书"."替代方案" IS '医生即将为患者实施的手术或有创性操作方案之外的其他方案，供患者选择';
COMMENT ON COLUMN "手术同意书"."医疗机构意见" IS '在此诊疗过程中，医疗机构对患者应尽责任的陈述以及可能面临的风险或意外情况所采取的应对措施的详细描述';
COMMENT ON COLUMN "手术同意书"."患者/法定代理人意见" IS '患者／法定代理人对知情同意书中告知内容的意见描述';
COMMENT ON COLUMN "手术同意书"."患者签名" IS '患者签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "手术同意书"."法定代理人姓名" IS '法定代理人签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "手术同意书"."法定代理人与患者的关系代码" IS '本人与特定对象的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "手术同意书"."法定代理人与患者的关系名称" IS '本人与特定对象的关系类别(如户主、配偶、子女、父母等)名称';
COMMENT ON COLUMN "手术同意书"."患者/法定代理人签名时间" IS '患者或法定代理人签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术同意书"."经治医师工号" IS '经治医师的工号';
COMMENT ON COLUMN "手术同意书"."经治医师姓名" IS '经治医师签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "手术同意书"."手术者工号" IS '实施手术的主要执行人员在原始特定编码体系中的编号';
COMMENT ON COLUMN "手术同意书"."手术者姓名" IS '实施手术的主要执行人员签署的在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "手术同意书"."医师签名时间" IS '医师进行电子签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术同意书"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "手术同意书"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术同意书"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术同意书"."手术同意书流水号" IS '按照某一特定编码规则赋予手术同意书的唯一标识';
COMMENT ON COLUMN "手术同意书"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "手术同意书"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "手术同意书"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "手术同意书"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "手术同意书"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "手术同意书"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "手术同意书"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
COMMENT ON COLUMN "手术同意书"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "手术同意书"."门诊就诊流水号" IS '为门诊就诊时，填写门诊就诊流水号，住院就诊流水号填“-”';
COMMENT ON COLUMN "手术同意书"."知情同意书编号" IS '按照某一特定编码规则赋予知情同意书的唯一标识';
COMMENT ON COLUMN "手术同意书"."就诊次数" IS '患者在医院就诊的次数';
COMMENT ON COLUMN "手术同意书"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON TABLE "手术同意书" IS '手术名称、禁忌症以及手术中可能出现意外风险的情况说明';


CREATE TABLE IF NOT EXISTS "性别重置术记录" (
"阴道直肠瘘标志" varchar (1) DEFAULT NULL,
 "尿瘘标志" varchar (1) DEFAULT NULL,
 "2周内发生感染标志" varchar (1) DEFAULT NULL,
 "患者对手术效果满意标志" varchar (1) DEFAULT NULL,
 "术后输血量" decimal (5,
 0) DEFAULT NULL,
 "术后输血标志" varchar (1) DEFAULT NULL,
 "自体血" decimal (5,
 0) DEFAULT NULL,
 "术中输血量" decimal (5,
 0) DEFAULT NULL,
 "术中输血标志" varchar (1) DEFAULT NULL,
 "术中出血量" decimal (5,
 0) DEFAULT NULL,
 "切除性腺标志" varchar (1) DEFAULT NULL,
 "住院药费" decimal (10,
 2) DEFAULT NULL,
 "住院费" decimal (18,
 3) DEFAULT NULL,
 "住院天数" decimal (5,
 0) DEFAULT NULL,
 "隆胸术类型代码" decimal (1,
 0) DEFAULT NULL,
 "外阴成形术类型代码" decimal (1,
 0) DEFAULT NULL,
 "阴道再造术类型代码" decimal (1,
 0) DEFAULT NULL,
 "脱毛标志" varchar (1) DEFAULT NULL,
 "面部轮廓成形术标志" varchar (1) DEFAULT NULL,
 "喉结整形标志" varchar (1) DEFAULT NULL,
 "外生殖器切除标志" varchar (1) DEFAULT NULL,
 "阴囊再造术类型代码" decimal (1,
 0) DEFAULT NULL,
 "阴茎再造术类型代码" decimal (1,
 0) DEFAULT NULL,
 "子宫附件切除标志" varchar (1) DEFAULT NULL,
 "乳房切除标志" varchar (1) DEFAULT NULL,
 "主刀医生姓名" varchar (50) DEFAULT NULL,
 "主刀医生工号" varchar (20) DEFAULT NULL,
 "手术名称3" varchar (100) DEFAULT NULL,
 "手术代码3" varchar (20) DEFAULT NULL,
 "手术结束时间3" timestamp DEFAULT NULL,
 "手术开始时间3" timestamp DEFAULT NULL,
 "手术名称2" varchar (100) DEFAULT NULL,
 "手术代码2" varchar (20) DEFAULT NULL,
 "手术结束时间2" timestamp DEFAULT NULL,
 "手术开始时间2" timestamp DEFAULT NULL,
 "手术名称1" varchar (100) DEFAULT NULL,
 "手术代码1" varchar (20) DEFAULT NULL,
 "手术结束时间1" timestamp DEFAULT NULL,
 "手术开始时间1" timestamp DEFAULT NULL,
 "诊断名称" varchar (100) DEFAULT NULL,
 "诊断代码" varchar (36) DEFAULT NULL,
 "性别重置术式" decimal (1,
 0) DEFAULT NULL,
 "术后性别代码" varchar (10) DEFAULT NULL,
 "术前性别代码" varchar (10) DEFAULT NULL,
 "手术后天数" decimal (10,
 0) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "手术申请单号" varchar (32) DEFAULT NULL,
 "手术记录流水号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "性别重置术记录编号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "填表时间" timestamp DEFAULT NULL,
 "填表人姓名" varchar (50) DEFAULT NULL,
 "填表人工号" varchar (20) DEFAULT NULL,
 "术后药物治疗标志" varchar (1) DEFAULT NULL,
 "再造阴茎感觉标志" varchar (1) DEFAULT NULL,
 "再造阴茎周径" decimal (5,
 3) DEFAULT NULL,
 "再造阴道深度" decimal (5,
 3) DEFAULT NULL,
 "再造阴道直径" decimal (5,
 3) DEFAULT NULL,
 "再造阴道狭窄标志" varchar (1) DEFAULT NULL,
 "再造尿道狭窄标志" varchar (1) DEFAULT NULL,
 "术后发生患者死亡标志" varchar (1) DEFAULT NULL,
 "皮瓣坏死标志" varchar (1) DEFAULT NULL,
 CONSTRAINT "性别重置术记录"_"性别重置术记录编号"_"医疗机构代码"_PK PRIMARY KEY ("性别重置术记录编号",
 "医疗机构代码")
);
COMMENT ON COLUMN "性别重置术记录"."皮瓣坏死标志" IS '标识是否发生皮瓣坏死的标志';
COMMENT ON COLUMN "性别重置术记录"."术后发生患者死亡标志" IS '标识术后是否发生患者死亡的标志';
COMMENT ON COLUMN "性别重置术记录"."再造尿道狭窄标志" IS '标识是否再造尿道狭窄的标志';
COMMENT ON COLUMN "性别重置术记录"."再造阴道狭窄标志" IS '标识是否再造阴道狭窄的标志';
COMMENT ON COLUMN "性别重置术记录"."再造阴道直径" IS '再造阴道直径的具体数值';
COMMENT ON COLUMN "性别重置术记录"."再造阴道深度" IS '再造阴道深度的具体数值';
COMMENT ON COLUMN "性别重置术记录"."再造阴茎周径" IS '再造阴茎周径的具体数值';
COMMENT ON COLUMN "性别重置术记录"."再造阴茎感觉标志" IS '再造阴茎有无感觉的情况标识';
COMMENT ON COLUMN "性别重置术记录"."术后药物治疗标志" IS '标识术后是否药物治疗的标志';
COMMENT ON COLUMN "性别重置术记录"."填表人工号" IS '填表人在原始特定编码体系中的编号';
COMMENT ON COLUMN "性别重置术记录"."填表人姓名" IS '填表人在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "性别重置术记录"."填表时间" IS '完成填表时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "性别重置术记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "性别重置术记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "性别重置术记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "性别重置术记录"."病房号" IS '患者入院时，所住病房对应的编号';
COMMENT ON COLUMN "性别重置术记录"."病床号" IS '患者入院时，所住床位对应的编号';
COMMENT ON COLUMN "性别重置术记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "性别重置术记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "性别重置术记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "性别重置术记录"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "性别重置术记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "性别重置术记录"."性别重置术记录编号" IS '按照某一特定编码规则赋予性别重置术记录的唯一标识';
COMMENT ON COLUMN "性别重置术记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "性别重置术记录"."手术记录流水号" IS '按照某一特定编码规则赋予手术记录的唯一标识';
COMMENT ON COLUMN "性别重置术记录"."手术申请单号" IS '按照某一特定编码规则赋予手术申请单的唯一标识';
COMMENT ON COLUMN "性别重置术记录"."就诊科室代码" IS '按照机构内编码规则赋予所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "性别重置术记录"."就诊科室名称" IS '所在科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "性别重置术记录"."病区名称" IS '患者入院时，所住病区名称';
COMMENT ON COLUMN "性别重置术记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "性别重置术记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "性别重置术记录"."住院次数" IS '此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "性别重置术记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "性别重置术记录"."出生日期" IS '患者出生当日的公元纪年日期';
COMMENT ON COLUMN "性别重置术记录"."ABO血型代码" IS '受检者按照ABO血型系统决定的血型在特定编码体系中的代码';
COMMENT ON COLUMN "性别重置术记录"."Rh血型代码" IS '进行血型检查明确，或既往病历资料能够明确的患者Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "性别重置术记录"."手术后天数" IS '手术后的具体天数';
COMMENT ON COLUMN "性别重置术记录"."术前性别代码" IS '术前生理性别代码';
COMMENT ON COLUMN "性别重置术记录"."术后性别代码" IS '术后生理性别代码';
COMMENT ON COLUMN "性别重置术记录"."性别重置术式" IS '性别重置术的特定分类';
COMMENT ON COLUMN "性别重置术记录"."诊断代码" IS '见《疾病分类及手术__疾病分类代码国家临床版_2.0》';
COMMENT ON COLUMN "性别重置术记录"."诊断名称" IS '诊断名称描述';
COMMENT ON COLUMN "性别重置术记录"."手术开始时间1" IS '以手术操作刀碰皮(切皮)时点计算，经自然腔道到达手术部位，没有皮肤切口的手术，开始时间以手术器械进入人体腔道的时点为准,对患者开始手术操作时的公元纪年日期和时间的完整描述1';
COMMENT ON COLUMN "性别重置术记录"."手术结束时间1" IS '对患者结束手术操作时的公元纪年日期和时间的完整描述1';
COMMENT ON COLUMN "性别重置术记录"."手术代码1" IS '按照某一特定编码规则赋予手术名称1的唯一标识';
COMMENT ON COLUMN "性别重置术记录"."手术名称1" IS '手术1的具体名称';
COMMENT ON COLUMN "性别重置术记录"."手术开始时间2" IS '以手术操作刀碰皮(切皮)时点计算，经自然腔道到达手术部位，没有皮肤切口的手术，开始时间以手术器械进入人体腔道的时点为准,对患者开始手术操作时的公元纪年日期和时间的完整描述2';
COMMENT ON COLUMN "性别重置术记录"."手术结束时间2" IS '对患者结束手术操作时的公元纪年日期和时间的完整描述2';
COMMENT ON COLUMN "性别重置术记录"."手术代码2" IS '按照某一特定编码规则赋予手术名称2的唯一标识';
COMMENT ON COLUMN "性别重置术记录"."手术名称2" IS '手术2的具体名称';
COMMENT ON COLUMN "性别重置术记录"."手术开始时间3" IS '以手术操作刀碰皮(切皮)时点计算，经自然腔道到达手术部位，没有皮肤切口的手术，开始时间以手术器械进入人体腔道的时点为准,对患者开始手术操作时的公元纪年日期和时间的完整描述3';
COMMENT ON COLUMN "性别重置术记录"."手术结束时间3" IS '对患者结束手术操作时的公元纪年日期和时间的完整描述3';
COMMENT ON COLUMN "性别重置术记录"."手术代码3" IS '按照某一特定编码规则赋予手术名称3的唯一标识';
COMMENT ON COLUMN "性别重置术记录"."手术名称3" IS '手术3的具体名称';
COMMENT ON COLUMN "性别重置术记录"."主刀医生工号" IS '实施手术的主要执行人员在原始特定编码体系中的编号';
COMMENT ON COLUMN "性别重置术记录"."主刀医生姓名" IS '实施手术的主要执行人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "性别重置术记录"."乳房切除标志" IS '标识是否乳房切除的标志';
COMMENT ON COLUMN "性别重置术记录"."子宫附件切除标志" IS '标识是否子宫附件切除的标志';
COMMENT ON COLUMN "性别重置术记录"."阴茎再造术类型代码" IS '阴茎再造术的类型代码';
COMMENT ON COLUMN "性别重置术记录"."阴囊再造术类型代码" IS '阴囊再造术的类型代码';
COMMENT ON COLUMN "性别重置术记录"."外生殖器切除标志" IS '标识是否外生殖器切除的标志';
COMMENT ON COLUMN "性别重置术记录"."喉结整形标志" IS '标识是否喉结整形的标志';
COMMENT ON COLUMN "性别重置术记录"."面部轮廓成形术标志" IS '标识是否面部轮廓成形术的标志';
COMMENT ON COLUMN "性别重置术记录"."脱毛标志" IS '标识是否脱毛的标志';
COMMENT ON COLUMN "性别重置术记录"."阴道再造术类型代码" IS '阴道再造术的类型代码';
COMMENT ON COLUMN "性别重置术记录"."外阴成形术类型代码" IS '外阴成形术的类型代码';
COMMENT ON COLUMN "性别重置术记录"."隆胸术类型代码" IS '隆胸术的类型代码';
COMMENT ON COLUMN "性别重置术记录"."住院天数" IS '患者实际的住院天数，入院日与出院日只计算1天';
COMMENT ON COLUMN "性别重置术记录"."住院费" IS '住院总费用，计量单位为人民币元';
COMMENT ON COLUMN "性别重置术记录"."住院药费" IS '住院费用中药品类的费用总额，计量单位为人民币元';
COMMENT ON COLUMN "性别重置术记录"."切除性腺标志" IS '标识是否切除性腺的标志';
COMMENT ON COLUMN "性别重置术记录"."术中出血量" IS '手术过程中出血总量，计量单位为mL';
COMMENT ON COLUMN "性别重置术记录"."术中输血标志" IS '标识是否术中输血的标志';
COMMENT ON COLUMN "性别重置术记录"."术中输血量" IS '手术过程中输血总量，计量单位为mL';
COMMENT ON COLUMN "性别重置术记录"."自体血" IS '自体血输血量，计量单位为mL';
COMMENT ON COLUMN "性别重置术记录"."术后输血标志" IS '标识是否术后输血的标志';
COMMENT ON COLUMN "性别重置术记录"."术后输血量" IS '手术后输血总量，计量单位为mL';
COMMENT ON COLUMN "性别重置术记录"."患者对手术效果满意标志" IS '标识患者对手术效果是否满意的标志';
COMMENT ON COLUMN "性别重置术记录"."2周内发生感染标志" IS '标识2周内是否发生感染的标志';
COMMENT ON COLUMN "性别重置术记录"."尿瘘标志" IS '标识是否发生尿瘘的标志';
COMMENT ON COLUMN "性别重置术记录"."阴道直肠瘘标志" IS '标识是否发生阴道直肠瘘的标志';
COMMENT ON TABLE "性别重置术记录" IS '性别重置术记录，包括手术名称、手术时间、阴茎再造术类型、术中输血信息、术中感染和手术医师信息';


CREATE TABLE IF NOT EXISTS "待产记录" (
"住院次数" decimal (5,
 0) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "待产记录流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "记录人工号" varchar (20) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "记录人姓名" varchar (50) DEFAULT NULL,
 "产程检查者姓名" varchar (50) DEFAULT NULL,
 "产程检查者工号" varchar (20) DEFAULT NULL,
 "产程经过" varchar (200) DEFAULT NULL,
 "产程记录时间" timestamp DEFAULT NULL,
 "拟分娩方式代码" varchar (2) DEFAULT NULL,
 "处置计划" varchar (200) DEFAULT NULL,
 "检查方式代码" varchar (1) DEFAULT NULL,
 "肠胀气标志" varchar (1) DEFAULT NULL,
 "膀胱充盈标志" varchar (1) DEFAULT NULL,
 "羊水情况" varchar (100) DEFAULT NULL,
 "先露位置" varchar (100) DEFAULT NULL,
 "破膜方式代码" varchar (1) DEFAULT NULL,
 "胎膜情况代码" varchar (1) DEFAULT NULL,
 "宫口情况" varchar (100) DEFAULT NULL,
 "宫颈情况" varchar (100) DEFAULT NULL,
 "宫缩情况" varchar (200) DEFAULT NULL,
 "坐骨结节间径(cm)" decimal (4,
 1) DEFAULT NULL,
 "骶耻外径(cm)" decimal (4,
 1) DEFAULT NULL,
 "头位难产情况的评估" varchar (200) DEFAULT NULL,
 "估计胎儿体重" decimal (4,
 0) DEFAULT NULL,
 "胎方位代码" varchar (2) DEFAULT NULL,
 "胎心率(次/min)" decimal (3,
 0) DEFAULT NULL,
 "腹围(cm)" decimal (5,
 1) DEFAULT NULL,
 "宫底高度(cm)" decimal (4,
 1) DEFAULT NULL,
 "呼吸频率(次/min)" decimal (3,
 0) DEFAULT NULL,
 "脉率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "既往孕产史" text,
 "手术史" text,
 "既往史" text,
 "此次妊娠特殊情况" varchar (200) DEFAULT NULL,
 "分娩前体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "身高(cm)" decimal (4,
 1) DEFAULT NULL,
 "孕前体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "产前检查异常情况" varchar (200) DEFAULT NULL,
 "产前检查标志" varchar (1) DEFAULT NULL,
 "预产期" date DEFAULT NULL,
 "受孕形式代码" varchar (1) DEFAULT NULL,
 "末次月经日期" date DEFAULT NULL,
 "产次" decimal (2,
 0) DEFAULT NULL,
 "孕次" decimal (2,
 0) DEFAULT NULL,
 "待产时间" timestamp DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "产妇姓名" varchar (50) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 CONSTRAINT "待产记录"_"待产记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("待产记录流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "待产记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "待产记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "待产记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "待产记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "待产记录"."产妇姓名" IS '产妇本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "待产记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "待产记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "待产记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "待产记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "待产记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "待产记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "待产记录"."待产时间" IS '产妇进入产房时的公元纪年日期和吋间的完整描述';
COMMENT ON COLUMN "待产记录"."孕次" IS '妊娠次数的累计值，包括异位妊娠，计量单位为次';
COMMENT ON COLUMN "待产记录"."产次" IS '产妇分娩总次数，包括28周后的引产，双多胎分娩只计1次';
COMMENT ON COLUMN "待产记录"."末次月经日期" IS '末次月经首日的公元纪年日期的完整描述';
COMMENT ON COLUMN "待产记录"."受孕形式代码" IS '受孕采取的形式在特定编码体系中的代码';
COMMENT ON COLUMN "待产记录"."预产期" IS '根据产妇末次月经首日推算的顶产期的公元纪年日期的完整描述';
COMMENT ON COLUMN "待产记录"."产前检查标志" IS '标识孕期产妇是否已进行产前检查的标志';
COMMENT ON COLUMN "待产记录"."产前检查异常情况" IS '产妇产前检查异常情况的详细描述';
COMMENT ON COLUMN "待产记录"."孕前体重(kg)" IS '产妇孕前体重的测量值，计量单位为kg';
COMMENT ON COLUMN "待产记录"."身高(cm)" IS '产妇身高的测量值，计量单位为cm';
COMMENT ON COLUMN "待产记录"."分娩前体重(kg)" IS '产妇分娩前体重的测量值，计量单位为kg';
COMMENT ON COLUMN "待产记录"."此次妊娠特殊情况" IS '对产妇此次妊娠特殊情况的详细描述';
COMMENT ON COLUMN "待产记录"."既往史" IS '既往健康状况及重要相关病史的描述';
COMMENT ON COLUMN "待产记录"."手术史" IS '对产妇既往接受手术/操作经历的详细描述';
COMMENT ON COLUMN "待产记录"."既往孕产史" IS '对产妇既往孕产史的详细描述';
COMMENT ON COLUMN "待产记录"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "待产记录"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "待产记录"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "待产记录"."脉率(次/min)" IS '产妇每分钟脉搏次数的测量值，计量单位为次/min';
COMMENT ON COLUMN "待产记录"."呼吸频率(次/min)" IS '单位时间内呼吸的次数,计壁单位为次/min';
COMMENT ON COLUMN "待产记录"."宫底高度(cm)" IS '受检者耻骨联合上缘至子宫底部距离的测量值,计量单位为cm';
COMMENT ON COLUMN "待产记录"."腹围(cm)" IS '产妇腹部经脐周长的测量值，计量单位为cm';
COMMENT ON COLUMN "待产记录"."胎心率(次/min)" IS '每分钟胎儿胎心搏动的次数，计量单位为次/min';
COMMENT ON COLUMN "待产记录"."胎方位代码" IS '胎儿方位的类别在特定编码体系中的代码';
COMMENT ON COLUMN "待产记录"."估计胎儿体重" IS '待产胎儿体重的估计值，计量单位为g';
COMMENT ON COLUMN "待产记录"."头位难产情况的评估" IS '对头位难产情况的评估';
COMMENT ON COLUMN "待产记录"."骶耻外径(cm)" IS '产妇第5腰椎棘突下至耻骨联合上缘中点距离的测量值，计量单位为cm';
COMMENT ON COLUMN "待产记录"."坐骨结节间径(cm)" IS '产妇两坐骨结节内侧缘的距离的测量值，又称骨盆出口横径，计量单位为cm';
COMMENT ON COLUMN "待产记录"."宫缩情况" IS '对产妇宮缩强弱、频率、持续时间等情况的洋细描述';
COMMENT ON COLUMN "待产记录"."宫颈情况" IS '产妇宫颈情况的详细描述';
COMMENT ON COLUMN "待产记录"."宫口情况" IS '产妇宫口扩张大小情况的详细描述';
COMMENT ON COLUMN "待产记录"."胎膜情况代码" IS '产妇胎膜是否已破裂的分类在特定编码体系中的代码';
COMMENT ON COLUMN "待产记录"."破膜方式代码" IS '采用的破膜方式类别的分类在特定编码体系中的代码';
COMMENT ON COLUMN "待产记录"."先露位置" IS '先露位置的详细描述';
COMMENT ON COLUMN "待产记录"."羊水情况" IS '羊水情况的详细描述';
COMMENT ON COLUMN "待产记录"."膀胱充盈标志" IS '标识膀胱是否充盈的标志';
COMMENT ON COLUMN "待产记录"."肠胀气标志" IS '标识产妇是否有肠胀气情况的标志';
COMMENT ON COLUMN "待产记录"."检查方式代码" IS '待产检查方式在特定编码体系中的代码';
COMMENT ON COLUMN "待产记录"."处置计划" IS '对产妇情况进行综合评估的基础上，为其制定的处置计划的详细描述';
COMMENT ON COLUMN "待产记录"."拟分娩方式代码" IS '计划选取的分娩方式类别(如阴道自然分娩、阴道手术助产、剖宫产等)在特定编码体系中的代码';
COMMENT ON COLUMN "待产记录"."产程记录时间" IS '产程记录完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "待产记录"."产程经过" IS '产程经过的详细描述';
COMMENT ON COLUMN "待产记录"."产程检查者工号" IS '产程检查者在机构内特定编码体系中的编号';
COMMENT ON COLUMN "待产记录"."产程检查者姓名" IS '产程检查者在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "待产记录"."记录人姓名" IS '记录人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "待产记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "待产记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "待产记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "待产记录"."记录人工号" IS '记录人员在机构内特定编码体系中的编号';
COMMENT ON COLUMN "待产记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "待产记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "待产记录"."待产记录流水号" IS '按照某一特定编码规则赋予待产记录的顺序号，是待产记录的唯一标识';
COMMENT ON COLUMN "待产记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "待产记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "待产记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "待产记录"."住院次数" IS '表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON TABLE "待产记录" IS '病史以及待产时各项检查结果';


CREATE TABLE IF NOT EXISTS "家庭医生签约日结数据_机构(
累计)" ("签约居民就诊人次数" decimal (10,
 0) DEFAULT NULL,
 "签约居民定点机构就诊人次数" decimal (10,
 0) DEFAULT NULL,
 "公共卫生服务经费金额" decimal (10,
 2) DEFAULT NULL,
 "财政投入金额" decimal (10,
 2) DEFAULT NULL,
 "医保基金金额" decimal (10,
 2) DEFAULT NULL,
 "家庭医生签约服务费" decimal (10,
 2) DEFAULT NULL,
 "下转的签约居民总数" decimal (10,
 0) DEFAULT NULL,
 "下转回访的签约居民人数" decimal (10,
 0) DEFAULT NULL,
 "签约协议完整的人数" decimal (10,
 0) DEFAULT NULL,
 "家庭医生重点人群签约数" decimal (10,
 0) DEFAULT NULL,
 "家庭医生签约总人数" decimal (10,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "统计日期" date NOT NULL,
 "机构名称" varchar (200) DEFAULT NULL,
 "机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "签约居民至其签约医生就诊次数" decimal (10,
 0) DEFAULT NULL,
 CONSTRAINT "家庭医生签约日结数据_机构(累计)"_"统计日期"_"机构代码"_PK PRIMARY KEY ("统计日期",
 "机构代码")
);
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."签约居民至其签约医生就诊次数" IS '截止统计日期，该机构签约居民至其签约医生就诊次数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."统计日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."家庭医生签约总人数" IS '截止统计日期，该机构家庭医生签约总人数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."家庭医生重点人群签约数" IS '截止统计日期，该机构家庭医生重点人群签约数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."签约协议完整的人数" IS '截止统计日期，该机构签约协议完整的人数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."下转回访的签约居民人数" IS '截止统计日期，该机构下转回访的签约居民人数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."下转的签约居民总数" IS '截止统计日期，该机构下转的签约居民总数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."家庭医生签约服务费" IS '截止统计日期，该机构家庭医生签约服务费';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."医保基金金额" IS '截止统计日期，该机构医保基金金额';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."财政投入金额" IS '截止统计日期，该机构财政投入金额';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."公共卫生服务经费金额" IS '截止统计日期，该机构公共卫生服务经费金额';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."签约居民定点机构就诊人次数" IS '截止统计日期，该机构签约居民定点机构就诊人次数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(累计)"."签约居民就诊人次数" IS '截止统计日期，该机构签约居民就诊人次数';
COMMENT ON TABLE "家庭医生签约日结数据_机构(累计)" IS '家庭医生机构签约累计日结数据，如签约总人数、重点人群签约数等';


CREATE TABLE IF NOT EXISTS "家庭医生签约日结数据_区域(
累计)" ("家庭医生重点人群签约数" decimal (10,
 0) DEFAULT NULL,
 "区划名称" varchar (12) NOT NULL,
 "统计日期" date NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "家庭医生签约总人数" decimal (10,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "签约居民至其签约医生就诊次数" decimal (10,
 0) DEFAULT NULL,
 "签约居民就诊人次数" decimal (10,
 0) DEFAULT NULL,
 "签约居民定点机构就诊人次数" decimal (10,
 0) DEFAULT NULL,
 "公共卫生服务经费金额" decimal (10,
 2) DEFAULT NULL,
 "财政投入金额" decimal (10,
 2) DEFAULT NULL,
 "医保基金金额" decimal (10,
 2) DEFAULT NULL,
 "家庭医生签约服务费" decimal (10,
 2) DEFAULT NULL,
 "下转的签约居民总数" decimal (10,
 0) DEFAULT NULL,
 "下转回访的签约居民人数" decimal (10,
 0) DEFAULT NULL,
 "签约协议完整的人数" decimal (10,
 0) DEFAULT NULL,
 "区划代码" varchar (12) NOT NULL,
 CONSTRAINT "家庭医生签约日结数据_区域(累计)"_"区划名称"_"统计日期"_"区划代码"_PK PRIMARY KEY ("区划名称",
 "统计日期",
 "区划代码")
);
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."区划代码" IS '医疗机构所在区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."签约协议完整的人数" IS '截止统计日期，该行政区划辖区签约协议完整的人数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."下转回访的签约居民人数" IS '截止统计日期，该行政区划辖区下转回访的签约居民人数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."下转的签约居民总数" IS '截止统计日期，该行政区划辖区下转的签约居民总数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."家庭医生签约服务费" IS '截止统计日期，该行政区划辖区家庭医生签约服务费';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."医保基金金额" IS '截止统计日期，该行政区划辖区医保基金金额';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."财政投入金额" IS '截止统计日期，该行政区划辖区财政投入金额';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."公共卫生服务经费金额" IS '截止统计日期，该行政区划辖区公共卫生服务经费金额';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."签约居民定点机构就诊人次数" IS '截止统计日期，该行政区划辖区签约居民定点机构就诊人次数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."签约居民就诊人次数" IS '截止统计日期，该行政区划辖区签约居民就诊人次数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."签约居民至其签约医生就诊次数" IS '截止统计日期，该行政区划辖区签约居民至其签约医生就诊次数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."家庭医生签约总人数" IS '截止统计日期，该行政区划辖区家庭医生签约总人数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."统计日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."区划名称" IS '医疗机构所在区划在机构内编码体系中的名称';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(累计)"."家庭医生重点人群签约数" IS '截止统计日期，该行政区划辖区家庭医生重点人群签约数';
COMMENT ON TABLE "家庭医生签约日结数据_区域(累计)" IS '家庭医生区域签约累计日结数据，如签约总人数、重点人群签约数等';


CREATE TABLE IF NOT EXISTS "实验室检验报告数量日汇总" (
"密级" varchar (16) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "业务日期" date NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "报告单总份数" decimal (9,
 0) DEFAULT NULL,
 "门诊报告单份数" decimal (9,
 0) DEFAULT NULL,
 "住院报告单份数" decimal (9,
 0) DEFAULT NULL,
 "其他业务报告单份数" decimal (9,
 0) DEFAULT NULL,
 "生化报告单份数" decimal (9,
 0) DEFAULT NULL,
 "细菌报告单份数" decimal (9,
 0) DEFAULT NULL,
 "药敏报告单份数" decimal (9,
 0) DEFAULT NULL,
 "其他报告单份数" decimal (9,
 0) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 CONSTRAINT "实验室检验报告数量日汇总"_"医疗机构代码"_"业务日期"_PK PRIMARY KEY ("医疗机构代码",
 "业务日期")
);
COMMENT ON COLUMN "实验室检验报告数量日汇总"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."其他报告单份数" IS '非生化、细菌、药敏报告的份数';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."药敏报告单份数" IS '日期内药敏检验报告的份数';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."细菌报告单份数" IS '日期内细菌检验报告的份数';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."生化报告单份数" IS '日期内除细菌、药敏以外的其他所有检验报告的份数';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."其他业务报告单份数" IS '日期内非门诊非住院类业务，例如体检类业务或外单位委托检验的检验报告总份数';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."住院报告单份数" IS '日期内住院类业务检验报告总份数';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."门诊报告单份数" IS '日期内门诊类业务检验报告总份数';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."报告单总份数" IS '日期内所有检验报告的总量';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."业务日期" IS '业务交易发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "实验室检验报告数量日汇总"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON TABLE "实验室检验报告数量日汇总" IS '按科室统计医院处方业务数据，包括处方总数和各类别药品处方数';


CREATE TABLE IF NOT EXISTS "基本医疗日结数据_机构(
周期)" ("密级" varchar (16) DEFAULT NULL,
 "贫困人口住院总费用" decimal (10,
 2) DEFAULT NULL,
 "贫困人口病人的住院自付费用" decimal (10,
 0) DEFAULT NULL,
 "基层医疗卫生机构住院人次数" decimal (10,
 0) DEFAULT NULL,
 "基层医疗卫生机构住院总费用" decimal (10,
 0) DEFAULT NULL,
 "基层医疗卫生机构门诊总人次数" decimal (10,
 0) DEFAULT NULL,
 "基层医疗卫生机构门诊总费用" decimal (10,
 0) DEFAULT NULL,
 "门诊用药中激素的处方数" decimal (10,
 0) DEFAULT NULL,
 "住院用药中有抗生素的次数" decimal (10,
 0) DEFAULT NULL,
 "门诊处方总数" decimal (10,
 0) DEFAULT NULL,
 "门诊用药中有抗生素的处方数" decimal (10,
 0) DEFAULT NULL,
 "基层医疗卫生机构出院人数" decimal (10,
 0) DEFAULT NULL,
 "基层医疗卫生机构出院者占用总床日数" decimal (10,
 0) DEFAULT NULL,
 "基层医疗卫生机构手术人次数" decimal (10,
 0) DEFAULT NULL,
 "基层医疗卫生机构入院人数" decimal (10,
 0) DEFAULT NULL,
 "基层医疗卫生机构中医诊疗人次数" decimal (10,
 0) DEFAULT NULL,
 "基层医疗卫生机构总诊疗人次数" decimal (10,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "统计日期" date NOT NULL,
 "机构名称" varchar (200) DEFAULT NULL,
 "机构代码" varchar (22) NOT NULL,
 "门诊静脉注射的处方数" decimal (10,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 CONSTRAINT "基本医疗日结数据_机构(周期)"_"统计日期"_"机构代码"_PK PRIMARY KEY ("统计日期",
 "机构代码")
);
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."门诊静脉注射的处方数" IS '统计周期内的基层医疗卫生机构门诊出具的静脉注射处方总数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."统计日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."基层医疗卫生机构总诊疗人次数" IS '统计周期内的基层医疗卫生机构总诊疗人次数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."基层医疗卫生机构中医诊疗人次数" IS '统计周期内的基层医疗卫生机构中医诊疗部分的人次数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."基层医疗卫生机构入院人数" IS '统计周期内的基层医疗卫生机构办理入院手续的人数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."基层医疗卫生机构手术人次数" IS '统计周期内的基层医疗卫生机构开展手术的人次数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."基层医疗卫生机构出院者占用总床日数" IS '统计周期内的基层医疗卫生机构出院者占用的总床日数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."基层医疗卫生机构出院人数" IS '统计周期内的基层医疗卫生机构出院的总人数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."门诊用药中有抗生素的处方数" IS '统计周期内的基层医疗卫生机构门诊出具的抗生素处方总数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."门诊处方总数" IS '统计周期内的基层医疗卫生机构门诊出具的处方总数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."住院用药中有抗生素的次数" IS '统计周期内的基层医疗卫生机构住院用药中有抗生素的次数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."门诊用药中激素的处方数" IS '统计周期内的基层医疗卫生机构门诊出具的激素用药总数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."基层医疗卫生机构门诊总费用" IS '统计周期内的基层医疗卫生机构门诊收入总费用';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."基层医疗卫生机构门诊总人次数" IS '统计周期内的基层医疗卫生机构门诊的人次数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."基层医疗卫生机构住院总费用" IS '统计周期内的基层医疗卫生机构住院收入总费用';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."基层医疗卫生机构住院人次数" IS '统计周期内的基层医疗卫生机构住院的人次数';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."贫困人口病人的住院自付费用" IS '统计周期内的基层医疗卫生机构贫困人口的住院费用中自付部分的费用';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."贫困人口住院总费用" IS '统计周期内的基层医疗卫生机构贫困人口的住院总费用';
COMMENT ON COLUMN "基本医疗日结数据_机构(周期)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON TABLE "基本医疗日结数据_机构(周期)" IS '基本医疗机构周期日结数据，如基层医疗卫生机构总诊疗人数、入院人数、处方数等';


CREATE TABLE IF NOT EXISTS "基本公卫日结数据_机构(
周期)" ("数据更新时间" timestamp DEFAULT NULL,
 "机构代码" varchar (22) NOT NULL,
 "机构名称" varchar (200) DEFAULT NULL,
 "统计日期" date NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "健康档案使用次数" decimal (10,
 0) DEFAULT NULL,
 "健康档案更新次数" decimal (10,
 0) DEFAULT NULL,
 "高血压患者随访次数" decimal (10,
 0) DEFAULT NULL,
 "糖尿病患者随访次数" decimal (10,
 0) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 CONSTRAINT "基本公卫日结数据_机构(周期)"_"机构代码"_"统计日期"_PK PRIMARY KEY ("机构代码",
 "统计日期")
);
COMMENT ON COLUMN "基本公卫日结数据_机构(周期)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本公卫日结数据_机构(周期)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "基本公卫日结数据_机构(周期)"."糖尿病患者随访次数" IS '统计周期内，该机构辖区糖尿病患者随访次数';
COMMENT ON COLUMN "基本公卫日结数据_机构(周期)"."高血压患者随访次数" IS '统计周期内，该机构辖区高血压患者随访次数';
COMMENT ON COLUMN "基本公卫日结数据_机构(周期)"."健康档案更新次数" IS '统计周期内，该机构辖区健康档案更新次数';
COMMENT ON COLUMN "基本公卫日结数据_机构(周期)"."健康档案使用次数" IS '统计周期内，该机构健康档案使用次数';
COMMENT ON COLUMN "基本公卫日结数据_机构(周期)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "基本公卫日结数据_机构(周期)"."统计日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "基本公卫日结数据_机构(周期)"."机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "基本公卫日结数据_机构(周期)"."机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "基本公卫日结数据_机构(周期)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON TABLE "基本公卫日结数据_机构(周期)" IS '基本公卫机构周期日结数据，如健康档案使用次数、健康方案更新次数、高血压患者随访次数等';


CREATE TABLE IF NOT EXISTS "基本公卫日结数据_区域(
周期)" ("区划代码" varchar (12) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "糖尿病患者随访次数" decimal (10,
 0) DEFAULT NULL,
 "高血压患者随访次数" decimal (10,
 0) DEFAULT NULL,
 "健康档案更新次数" decimal (10,
 0) DEFAULT NULL,
 "健康档案使用次数" decimal (10,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "统计日期" date NOT NULL,
 "区划名称" varchar (12) NOT NULL,
 CONSTRAINT "基本公卫日结数据_区域(周期)"_"区划代码"_"统计日期"_"区划名称"_PK PRIMARY KEY ("区划代码",
 "统计日期",
 "区划名称")
);
COMMENT ON COLUMN "基本公卫日结数据_区域(周期)"."区划名称" IS '医疗机构所在区划在机构内编码体系中的名称';
COMMENT ON COLUMN "基本公卫日结数据_区域(周期)"."统计日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "基本公卫日结数据_区域(周期)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "基本公卫日结数据_区域(周期)"."健康档案使用次数" IS '统计周期内，该行政区域辖区健康档案使用次数';
COMMENT ON COLUMN "基本公卫日结数据_区域(周期)"."健康档案更新次数" IS '统计周期内，该行政区域辖区健康档案更新次数';
COMMENT ON COLUMN "基本公卫日结数据_区域(周期)"."高血压患者随访次数" IS '统计周期内，该行政区域辖区高血压患者随访次数';
COMMENT ON COLUMN "基本公卫日结数据_区域(周期)"."糖尿病患者随访次数" IS '统计周期内，该行政区域辖区糖尿病患者随访次数';
COMMENT ON COLUMN "基本公卫日结数据_区域(周期)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "基本公卫日结数据_区域(周期)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本公卫日结数据_区域(周期)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本公卫日结数据_区域(周期)"."区划代码" IS '医疗机构所在区划在机构内编码体系中的唯一标识';
COMMENT ON TABLE "基本公卫日结数据_区域(周期)" IS '基本公卫区域周期日结数据，如健康档案使用次数、健康方案更新次数、高血压患者随访次数等';


CREATE TABLE IF NOT EXISTS "同种胰岛移植术记录" (
"胰岛移植围手术期死亡标志" varchar (1) DEFAULT NULL,
 "检验结果-并发症-其他" varchar (500) DEFAULT NULL,
 "检验结果-并发症" decimal (1,
 0) DEFAULT NULL,
 "有胰岛移植手术直接相关的并发症标志" varchar (1) DEFAULT NULL,
 "检验结果-内毒素超标标志" varchar (1) DEFAULT NULL,
 "胰岛产物内毒素检验标志" varchar (1) DEFAULT NULL,
 "支原体培养结果" decimal (1,
 0) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "真菌培养结果" decimal (1,
 0) DEFAULT NULL,
 "细菌培养结果" decimal (1,
 0) DEFAULT NULL,
 "胰岛产物微生物培养标志" varchar (1) DEFAULT NULL,
 "胰岛活率(%)" decimal (5,
 2) DEFAULT NULL,
 "胰岛纯度(%)" decimal (5,
 2) DEFAULT NULL,
 "胰岛总当量(mL)" decimal (3,
 0) DEFAULT NULL,
 "植入胰岛数" decimal (1,
 0) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "手术申请单号" varchar (32) DEFAULT NULL,
 "结束手术时间" timestamp DEFAULT NULL,
 "供者个数" decimal (5,
 0) DEFAULT NULL,
 "移植方式1" decimal (1,
 0) DEFAULT NULL,
 "移植方式2" decimal (1,
 0) DEFAULT NULL,
 "移植方式3" decimal (1,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "开始手术时间" timestamp DEFAULT NULL,
 "HLA分型" decimal (1,
 0) DEFAULT NULL,
 "其他诊断" varchar (500) DEFAULT NULL,
 "肾功能衰竭标志" varchar (1) DEFAULT NULL,
 "胰腺切除术后糖尿病标志" varchar (1) DEFAULT NULL,
 "器官移植后糖尿病标志" varchar (1) DEFAULT NULL,
 "糖尿病类型" decimal (1,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "与器官移植手术联合进行标志" varchar (1) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "同种胰岛移植术记录编号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "手术记录流水号" varchar (64) DEFAULT NULL,
 "胰岛移植围手术期死亡原因" varchar (500) DEFAULT NULL,
 "住院天数" decimal (5,
 0) DEFAULT NULL,
 "移植后天数" decimal (20,
 0) DEFAULT NULL,
 "生存标志" varchar (1) DEFAULT NULL,
 "死亡时间" timestamp DEFAULT NULL,
 "死亡原因" text,
 "糖基化血红蛋白小于7.0%标志" varchar (1) DEFAULT NULL,
 "糖基化血红蛋白具体数值" decimal (6,
 2) DEFAULT NULL,
 "低血糖标志" varchar (1) DEFAULT NULL,
 "血糖值(mmol/L)" decimal (6,
 2) DEFAULT NULL,
 "血清C-肽水平不低于0.3ng标志" varchar (1) DEFAULT NULL,
 "血清C-肽水平具体数值" decimal (6,
 2) DEFAULT NULL,
 "主刀医生工号" varchar (20) DEFAULT NULL,
 "主刀医生姓名" varchar (50) DEFAULT NULL,
 "填表人工号" varchar (20) DEFAULT NULL,
 "填表人姓名" varchar (50) DEFAULT NULL,
 "填表时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 CONSTRAINT "同种胰岛移植术记录"_"医疗机构代码"_"同种胰岛移植术记录编号"_PK PRIMARY KEY ("医疗机构代码",
 "同种胰岛移植术记录编号")
);
COMMENT ON COLUMN "同种胰岛移植术记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "同种胰岛移植术记录"."填表时间" IS '完成填表时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种胰岛移植术记录"."填表人姓名" IS '填表人在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "同种胰岛移植术记录"."填表人工号" IS '填表人在原始特定编码体系中的编号';
COMMENT ON COLUMN "同种胰岛移植术记录"."主刀医生姓名" IS '实施手术的主要执行人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "同种胰岛移植术记录"."主刀医生工号" IS '实施手术的主要执行人员在原始特定编码体系中的编号';
COMMENT ON COLUMN "同种胰岛移植术记录"."血清C-肽水平具体数值" IS '血清C-肽水平的具体数值';
COMMENT ON COLUMN "同种胰岛移植术记录"."血清C-肽水平不低于0.3ng标志" IS '标识是否血清C-肽水平不低于0.3ng的标志';
COMMENT ON COLUMN "同种胰岛移植术记录"."血糖值(mmol/L)" IS '血液中葡萄糖定量检测结果值';
COMMENT ON COLUMN "同种胰岛移植术记录"."低血糖标志" IS '标识是否发生低血糖的标志';
COMMENT ON COLUMN "同种胰岛移植术记录"."糖基化血红蛋白具体数值" IS '糖基化血红蛋白的具体数值';
COMMENT ON COLUMN "同种胰岛移植术记录"."糖基化血红蛋白小于7.0%标志" IS '标识是否糖基化血红蛋白是否小于7.0%的标志';
COMMENT ON COLUMN "同种胰岛移植术记录"."死亡原因" IS '患者死亡原因的详细描述';
COMMENT ON COLUMN "同种胰岛移植术记录"."死亡时间" IS '患者死亡当时的公元纪年和日期的完整描述';
COMMENT ON COLUMN "同种胰岛移植术记录"."生存标志" IS '标识是否生存的标志';
COMMENT ON COLUMN "同种胰岛移植术记录"."移植后天数" IS '移植后的天数';
COMMENT ON COLUMN "同种胰岛移植术记录"."住院天数" IS '患者实际的住院天数，入院日与出院日只计算1天';
COMMENT ON COLUMN "同种胰岛移植术记录"."胰岛移植围手术期死亡原因" IS '胰岛移植围手术期死亡原因的详细描述';
COMMENT ON COLUMN "同种胰岛移植术记录"."手术记录流水号" IS '按照某一特定编码规则赋予手术记录的唯一标识';
COMMENT ON COLUMN "同种胰岛移植术记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "同种胰岛移植术记录"."同种胰岛移植术记录编号" IS '按照某一特定编码规则赋予同种胰岛移植术记录的唯一标识';
COMMENT ON COLUMN "同种胰岛移植术记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "同种胰岛移植术记录"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "同种胰岛移植术记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "同种胰岛移植术记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "同种胰岛移植术记录"."与器官移植手术联合进行标志" IS '标识是否与器官移植手术联合进行的标志';
COMMENT ON COLUMN "同种胰岛移植术记录"."ABO血型代码" IS '受检者按照ABO血型系统决定的血型在特定编码体系中的代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."Rh血型代码" IS '进行血型检查明确，或既往病历资料能够明确的患者Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."住院次数" IS '此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "同种胰岛移植术记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "同种胰岛移植术记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "同种胰岛移植术记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."出生日期" IS '患者出生当日的公元纪年日期';
COMMENT ON COLUMN "同种胰岛移植术记录"."糖尿病类型" IS '糖尿病类型在特定编码体系中的代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."器官移植后糖尿病标志" IS '器官移植后是否诊断出糖尿病';
COMMENT ON COLUMN "同种胰岛移植术记录"."胰腺切除术后糖尿病标志" IS '胰岛切除后是否诊断出糖尿病';
COMMENT ON COLUMN "同种胰岛移植术记录"."肾功能衰竭标志" IS '是否肾功能衰竭';
COMMENT ON COLUMN "同种胰岛移植术记录"."其他诊断" IS '其他诊断的描述';
COMMENT ON COLUMN "同种胰岛移植术记录"."HLA分型" IS '白细胞抗原分型在特定编码体系中的代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."开始手术时间" IS '以手术操作刀碰皮(切皮)时点计算，经自然腔道到达手术部位，没有皮肤切口的手术，开始时间以手术器械进入人体腔道的时点为准,对患者开始手术操作时的公元纪年日期和时间的完整描述。';
COMMENT ON COLUMN "同种胰岛移植术记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种胰岛移植术记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种胰岛移植术记录"."移植方式3" IS '当移植方式 1选择 1 时，移植方式 3 必填。0：腹部手术中门静 脉属支同种 胰岛移植；1： 腹部手术中 肾包膜下同 种胰岛移植； 2：腹部手术中胰腺被摸 或大网膜内 同种胰岛移 植';
COMMENT ON COLUMN "同种胰岛移植术记录"."移植方式2" IS '当移植方式 1 选择 0，移植方式 2 必填。0：经皮经肝门静脉穿刺介入种种胰 岛移植；1： 经股动脉肝 动脉置管同种胰岛移植';
COMMENT ON COLUMN "同种胰岛移植术记录"."移植方式1" IS '介入或手术移植方式在特定编码体系中的代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."供者个数" IS '供者数量，计量单位为个';
COMMENT ON COLUMN "同种胰岛移植术记录"."结束手术时间" IS '对患者结束手术操作时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种胰岛移植术记录"."手术申请单号" IS '按照某一特定编码规则赋予手术申请单的唯一标识';
COMMENT ON COLUMN "同种胰岛移植术记录"."就诊科室代码" IS '按照机构内编码规则赋予所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "同种胰岛移植术记录"."就诊科室名称" IS '所在科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "同种胰岛移植术记录"."病区名称" IS '患者入院时，所住病区名称';
COMMENT ON COLUMN "同种胰岛移植术记录"."病房号" IS '患者入院时，所住病房对应的编号';
COMMENT ON COLUMN "同种胰岛移植术记录"."病床号" IS '患者入院时，所住床位对应的编号';
COMMENT ON COLUMN "同种胰岛移植术记录"."植入胰岛数" IS '植入胰岛的数量';
COMMENT ON COLUMN "同种胰岛移植术记录"."胰岛总当量(mL)" IS '一个直径 150 μm 的胰岛为1 个胰岛当量，单位：mL';
COMMENT ON COLUMN "同种胰岛移植术记录"."胰岛纯度(%)" IS '胰岛纯度是指DTZ 染色阳性的胰岛数占纯化的细胞团总数的比例，单位：%';
COMMENT ON COLUMN "同种胰岛移植术记录"."胰岛活率(%)" IS '活胰岛数与胰岛总数的比，单位：%';
COMMENT ON COLUMN "同种胰岛移植术记录"."胰岛产物微生物培养标志" IS '标识是否进行胰岛产物微生物培养的标志';
COMMENT ON COLUMN "同种胰岛移植术记录"."细菌培养结果" IS '胰岛产物微生物细菌培养的结果代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."真菌培养结果" IS '胰岛产物微生物真菌培养的结果代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "同种胰岛移植术记录"."支原体培养结果" IS '胰岛产物微生物支原体培养的结果代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."胰岛产物内毒素检验标志" IS '标识是否进行胰岛产物内毒素检验的标志';
COMMENT ON COLUMN "同种胰岛移植术记录"."检验结果-内毒素超标标志" IS '内毒素检验结果是否超标的代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."有胰岛移植手术直接相关的并发症标志" IS '有无胰岛移植手术直接相关的并发症的情况代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."检验结果-并发症" IS '并发症类型在特定编码体系中的代码';
COMMENT ON COLUMN "同种胰岛移植术记录"."检验结果-并发症-其他" IS '其他并发症检验结果的详细描述';
COMMENT ON COLUMN "同种胰岛移植术记录"."胰岛移植围手术期死亡标志" IS '标识是否胰岛移植围手术期死亡的标志。围手术期并_发症是指同_种胰岛移植_治疗术后_30_天内发生的_并发症，包括出血、感染、门静脉血栓形成等';
COMMENT ON TABLE "同种胰岛移植术记录" IS '同种胰岛移植术记录，包括糖尿病类型、HLA分型、移植方式、胰岛产物微生物培养结果、并发症以及死亡信息';


CREATE TABLE IF NOT EXISTS "同种异体角膜移植术记录" (
"其他移植方式标志" varchar (1) DEFAULT NULL,
 "术后植片状况" decimal (1,
 0) DEFAULT NULL,
 "术后并发症标志" varchar (1) DEFAULT NULL,
 "术后并发症情况" decimal (1,
 0) DEFAULT NULL,
 "角膜移植成功标志" varchar (1) DEFAULT NULL,
 "角膜植片透明标志" varchar (1) DEFAULT NULL,
 "角膜原发疾病控制标志" varchar (1) DEFAULT NULL,
 "术后视力提高标志" varchar (1) DEFAULT NULL,
 "术前术后诊断符合标志" varchar (1) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "移植术记录编号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "手术记录流水号" varchar (64) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "填表时间" timestamp DEFAULT NULL,
 "填表人姓名" varchar (50) DEFAULT NULL,
 "填表人工号" varchar (20) DEFAULT NULL,
 "手术医生姓名" varchar (50) DEFAULT NULL,
 "手术医生工号" varchar (20) DEFAULT NULL,
 "并发症描述" varchar (500) DEFAULT NULL,
 "其他并发症标志" varchar (1) DEFAULT NULL,
 "继发青光眼原发病复发标志" varchar (1) DEFAULT NULL,
 "植片感染标志" varchar (1) DEFAULT NULL,
 "其他移植具体方式" varchar (500) DEFAULT NULL,
 "手术申请单号" varchar (32) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "受体本地唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "受体姓名" varchar (50) DEFAULT NULL,
 "受体性别代码" varchar (10) DEFAULT NULL,
 "受体出生日期" date DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "受体入院诊断" varchar (1) DEFAULT NULL,
 "受体入院诊断_感染性疾病" varchar (1) DEFAULT NULL,
 "受体入院诊断_外伤性疾病" varchar (1) DEFAULT NULL,
 "受体入院诊断_先天性疾病" varchar (1) DEFAULT NULL,
 "受体入院诊断_其他疾病" varchar (1) DEFAULT NULL,
 "受体移植前疾病状态" decimal (1,
 0) DEFAULT NULL,
 "受体出院诊断" varchar (1) DEFAULT NULL,
 "受体出院诊断_感染性疾病" varchar (1) DEFAULT NULL,
 "受体出院诊断_外伤性疾病" varchar (1) DEFAULT NULL,
 "受体出院诊断_先天性疾病" varchar (1) DEFAULT NULL,
 "受体出院诊断_其他疾病" varchar (1) DEFAULT NULL,
 "手术开始时间" timestamp DEFAULT NULL,
 "手术结束时间" timestamp DEFAULT NULL,
 "采集时间" timestamp DEFAULT NULL,
 "保存方式" decimal (1,
 0) DEFAULT NULL,
 "角膜供体的透明性" decimal (1,
 0) DEFAULT NULL,
 "角膜内皮移植标志" varchar (1) DEFAULT NULL,
 "穿透角膜移植标志" varchar (1) DEFAULT NULL,
 "板层角膜移植标志" varchar (1) DEFAULT NULL,
 "角膜缘干细胞移植标志" varchar (1) DEFAULT NULL,
 CONSTRAINT "同种异体角膜移植术记录"_"医疗机构代码"_"移植术记录编号"_PK PRIMARY KEY ("医疗机构代码",
 "移植术记录编号")
);
COMMENT ON COLUMN "同种异体角膜移植术记录"."角膜缘干细胞移植标志" IS '是否角膜缘干细胞移植';
COMMENT ON COLUMN "同种异体角膜移植术记录"."板层角膜移植标志" IS '是否板层角膜移植';
COMMENT ON COLUMN "同种异体角膜移植术记录"."穿透角膜移植标志" IS '是否穿透角膜移植';
COMMENT ON COLUMN "同种异体角膜移植术记录"."角膜内皮移植标志" IS '是否角膜内皮移植';
COMMENT ON COLUMN "同种异体角膜移植术记录"."角膜供体的透明性" IS '角膜供体的透明性在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."保存方式" IS '角膜采集后的保存方式在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."采集时间" IS '采集完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种异体角膜移植术记录"."手术结束时间" IS '对患者结束手术操作时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种异体角膜移植术记录"."手术开始时间" IS '以手术操作刀碰皮(切皮)时点计算，经自然腔道到达手术部位，没有皮肤切口的手术，开始时间以手术器械进入人体腔道的时点为准,对患者开始手术操作时的公元纪年日期和时间的完整描述。';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体出院诊断_其他疾病" IS '受体出院诊断中除上述类型外，其他疾病的特定代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体出院诊断_先天性疾病" IS '受体出院诊断中先天性疾病的特定代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体出院诊断_外伤性疾病" IS '受体出院诊断中外伤性疾病的特定代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体出院诊断_感染性疾病" IS '受体出院诊断中感染性疾病的特定代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体出院诊断" IS '受体出院诊断在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体移植前疾病状态" IS '受体移植前疾病状态的特定代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体入院诊断_其他疾病" IS '当受体入院诊断填3_时，必填。0：角膜内皮失代_偿；1：角膜白斑；2：角膜边缘溃疡；_3：角膜烧伤；4：角膜穿孔';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体入院诊断_先天性疾病" IS '当受体入院_诊断填_2_时，_必填。0：角膜皮样瘤；1：_圆锥角膜；2：_角膜营养不良';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体入院诊断_外伤性疾病" IS '当受体入院诊断填1_时，必填。0：角膜疤痕、1：角膜失代偿';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体入院诊断_感染性疾病" IS '当受体入院诊断填0_时，必填。0：细菌性角膜溃_疡；1：真菌性角膜溃疡；2：病毒性角膜溃疡；3：_棘阿米巴角膜溃疡';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体入院诊断" IS '受体入院诊断在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."Rh血型代码" IS '进行血型检查明确，或既往病历资料能够明确的患者Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."ABO血型代码" IS '受检者按照ABO血型系统决定的血型在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体出生日期" IS '患者出生当日的公元纪年日期';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "同种异体角膜移植术记录"."住院次数" IS '此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "同种异体角膜移植术记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "同种异体角膜移植术记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "同种异体角膜移植术记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "同种异体角膜移植术记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."受体本地唯一标识号" IS '就诊机构代码+受体本地唯一 ID 关联患者基本信息';
COMMENT ON COLUMN "同种异体角膜移植术记录"."病床号" IS '患者入院时，所住床位对应的编号';
COMMENT ON COLUMN "同种异体角膜移植术记录"."病房号" IS '患者入院时，所住病房对应的编号';
COMMENT ON COLUMN "同种异体角膜移植术记录"."病区名称" IS '患者入院时，所住病区名称';
COMMENT ON COLUMN "同种异体角膜移植术记录"."就诊科室名称" IS '所在科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "同种异体角膜移植术记录"."就诊科室代码" IS '按照机构内编码规则赋予所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "同种异体角膜移植术记录"."手术申请单号" IS '按照某一特定编码规则赋予手术申请单的唯一标识';
COMMENT ON COLUMN "同种异体角膜移植术记录"."其他移植具体方式" IS '其他移植方式的详细描述';
COMMENT ON COLUMN "同种异体角膜移植术记录"."植片感染标志" IS '标识是否植片感染的标志';
COMMENT ON COLUMN "同种异体角膜移植术记录"."继发青光眼原发病复发标志" IS '标识是否继发青光眼原发病复发的标志';
COMMENT ON COLUMN "同种异体角膜移植术记录"."其他并发症标志" IS '标识是否其他并发症的标志';
COMMENT ON COLUMN "同种异体角膜移植术记录"."并发症描述" IS '并发症的详细描述';
COMMENT ON COLUMN "同种异体角膜移植术记录"."手术医生工号" IS '手术医生在原始特定编码体系中的编号';
COMMENT ON COLUMN "同种异体角膜移植术记录"."手术医生姓名" IS '手术医生在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "同种异体角膜移植术记录"."填表人工号" IS '填表人在原始特定编码体系中的编号';
COMMENT ON COLUMN "同种异体角膜移植术记录"."填表人姓名" IS '填表人在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "同种异体角膜移植术记录"."填表时间" IS '完成填表时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种异体角膜移植术记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "同种异体角膜移植术记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种异体角膜移植术记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种异体角膜移植术记录"."手术记录流水号" IS '按照某一特定编码规则赋予手术记录的唯一标识';
COMMENT ON COLUMN "同种异体角膜移植术记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "同种异体角膜移植术记录"."移植术记录编号" IS '按照某一特定编码规则赋予移植术记录的唯一标识';
COMMENT ON COLUMN "同种异体角膜移植术记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "同种异体角膜移植术记录"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "同种异体角膜移植术记录"."术前术后诊断符合标志" IS '标识术前术后诊断是否符合的标志';
COMMENT ON COLUMN "同种异体角膜移植术记录"."术后视力提高标志" IS '术后是否视力提高的情况标识';
COMMENT ON COLUMN "同种异体角膜移植术记录"."角膜原发疾病控制标志" IS '标识是否角膜原发疾病控制的标志';
COMMENT ON COLUMN "同种异体角膜移植术记录"."角膜植片透明标志" IS '标识是否角膜植片透明的标志';
COMMENT ON COLUMN "同种异体角膜移植术记录"."角膜移植成功标志" IS '标识是否角膜移植成功的标志';
COMMENT ON COLUMN "同种异体角膜移植术记录"."术后并发症情况" IS '术后并发症的情况代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."术后并发症标志" IS '术后有无并发症的情况标识';
COMMENT ON COLUMN "同种异体角膜移植术记录"."术后植片状况" IS '术后植片状况的特定代码';
COMMENT ON COLUMN "同种异体角膜移植术记录"."其他移植方式标志" IS '是否有其他移植方式';
COMMENT ON TABLE "同种异体角膜移植术记录" IS '同种异体角膜移植术记录，包括入院诊断、出院诊断、角膜移植类型、并发症信息';


CREATE TABLE IF NOT EXISTS "卫生监督协管巡查登记表" (
"报告流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "年度" decimal (4,
 0) DEFAULT NULL,
 "序号" varchar (50) DEFAULT NULL,
 "巡查地点与内容" varchar (500) DEFAULT NULL,
 "发现主要问题" text,
 "巡查日期" date DEFAULT NULL,
 "巡查人姓名" varchar (50) DEFAULT NULL,
 "巡查人工号" varchar (20) DEFAULT NULL,
 "备注" varchar (1000) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "机构代码" varchar (22) NOT NULL,
 "机构名称" varchar (200) DEFAULT NULL,
 CONSTRAINT "卫生监督协管巡查登记表"_"报告流水号"_"机构代码"_PK PRIMARY KEY ("报告流水号",
 "机构代码")
);
COMMENT ON COLUMN "卫生监督协管巡查登记表"."机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."备注" IS '重要信息提示和补充说明';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."巡查人工号" IS '巡查人在机构特定编码体系中的编号';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."巡查人姓名" IS '巡查人在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."巡查日期" IS '巡查当天的公元纪年日期';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."发现主要问题" IS '监督协管巡查发现主要问题的详细描述';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."巡查地点与内容" IS '监督协管巡查地点与巡查内容的详细描述';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."序号" IS '按照某一特定规则赋予登记信息在监督协管巡查登记表单中的顺序号';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."年度" IS '巡查登记信息报告年份';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "卫生监督协管巡查登记表"."报告流水号" IS '按照一定编码赋予报告卡的顺序号';
COMMENT ON TABLE "卫生监督协管巡查登记表" IS '卫生监督协管巡查登记，包括巡查地点与内容、发现的主要问题等';


CREATE TABLE IF NOT EXISTS "医院感染记录" (
"侵袭性操作选择器代码" varchar (50) DEFAULT NULL,
 "抗生素使用情况" varchar (30) DEFAULT NULL,
 "抗生素使用情况代码" varchar (2) DEFAULT NULL,
 "其他易感因素描述" varchar (1000) DEFAULT NULL,
 "易感因素名称" varchar (1000) DEFAULT NULL,
 "易感因素代码" varchar (50) DEFAULT NULL,
 "医院感染与原发病预后的关系名称" varchar (30) DEFAULT NULL,
 "医院感染与原发病预后的关系代码" varchar (2) DEFAULT NULL,
 "病原学检查标志" varchar (1) DEFAULT NULL,
 "感染预后" varchar (30) DEFAULT NULL,
 "感染预后代码" varchar (2) DEFAULT NULL,
 "感染科室名称" varchar (100) DEFAULT NULL,
 "感染科室代码" varchar (20) DEFAULT NULL,
 "新生儿感染标志" varchar (1) DEFAULT NULL,
 "其他感染部位描述" varchar (100) DEFAULT NULL,
 "感染部位名称" varchar (30) DEFAULT NULL,
 "感染部位代码" varchar (2) DEFAULT NULL,
 "感染类型名称" varchar (30) DEFAULT NULL,
 "感染类型代码" varchar (2) DEFAULT NULL,
 "感染日期" date DEFAULT NULL,
 "住院患者出院病室名称" varchar (50) DEFAULT NULL,
 "住院患者出院病室代码" varchar (30) DEFAULT NULL,
 "出院诊断名称" varchar (1000) DEFAULT NULL,
 "出院诊断代码" varchar (100) DEFAULT NULL,
 "出院时间" timestamp DEFAULT NULL,
 "住院患者入院科室名称" varchar (100) DEFAULT NULL,
 "住院患者入院科室代码" varchar (20) DEFAULT NULL,
 "其他侵袭性操作选择器" varchar (1000) DEFAULT NULL,
 "控制方法代码" varchar (30) DEFAULT NULL,
 "检查方法代码" varchar (2) DEFAULT NULL,
 "检查方法名称" varchar (500) DEFAULT NULL,
 "药敏实验标志" varchar (1) DEFAULT NULL,
 "药敏情况" varchar (200) DEFAULT NULL,
 "住院机构代码" varchar (50) DEFAULT NULL,
 "住院机构名称" varchar (70) DEFAULT NULL,
 "文本内容" text,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "入院诊断描述" varchar (200) DEFAULT NULL,
 "入院诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "感染记录编号" varchar (36) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "医院感染记录流水号" varchar (32) NOT NULL,
 "上传机构名称" varchar (70) DEFAULT NULL,
 "上传机构代码" varchar (50) NOT NULL,
 "控制方法名称" varchar (20) DEFAULT NULL,
 "侵袭性操作选择器名称" varchar (1000) DEFAULT NULL,
 CONSTRAINT "医院感染记录"_"医院感染记录流水号"_"上传机构代码"_PK PRIMARY KEY ("医院感染记录流水号",
 "上传机构代码")
);
COMMENT ON COLUMN "医院感染记录"."侵袭性操作选择器名称" IS '侵袭性操作选择器在特定编码体系中的名称，如内窥镜、透析疗法等。可以多个，以“，”分割';
COMMENT ON COLUMN "医院感染记录"."控制方法名称" IS '控制方法在特定编码体系中的名称，如药物治疗、隔离等。可以多个，以“，”分割';
COMMENT ON COLUMN "医院感染记录"."上传机构代码" IS '上报机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "医院感染记录"."上传机构名称" IS '上报医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "医院感染记录"."医院感染记录流水号" IS '按照某一特性编码规则赋予本次感染记录的唯一标识';
COMMENT ON COLUMN "医院感染记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "医院感染记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "医院感染记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "医院感染记录"."就诊次数" IS '就诊次数，即“第次就诊”指患者在本医疗机构诊治的次数。计量单位为次';
COMMENT ON COLUMN "医院感染记录"."姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医院感染记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染记录"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "医院感染记录"."年龄(岁)" IS '患者年龄满1?周岁的实足年龄，为患者出生后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "医院感染记录"."年龄(月)" IS '条件必填，年龄不足1?周岁的实足年龄的月龄，以分数形式表示：分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1?个月的天数。此时患者年龄(岁)填写值为“0”';
COMMENT ON COLUMN "医院感染记录"."住院号" IS '能标识一次住院业务的就诊流水号';
COMMENT ON COLUMN "医院感染记录"."感染记录编号" IS '能标识患者发生一次院感的记录编号';
COMMENT ON COLUMN "医院感染记录"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院感染记录"."入院诊断-西医诊断代码" IS '由医师根据患者入院时的情况，综合分析所作出诊断在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "医院感染记录"."入院诊断描述" IS '由医师根据患者入院时的情况，综合分析所作出诊断在机构内编码体系中的名称';
COMMENT ON COLUMN "医院感染记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院感染记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院感染记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "医院感染记录"."文本内容" IS '文本详细内容';
COMMENT ON COLUMN "医院感染记录"."住院机构名称" IS '住院医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "医院感染记录"."住院机构代码" IS '住院机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "医院感染记录"."药敏情况" IS '药敏试验情况描述';
COMMENT ON COLUMN "医院感染记录"."药敏实验标志" IS '标识是否做药敏试验';
COMMENT ON COLUMN "医院感染记录"."检查方法名称" IS '检查方法在特定编码体系中的名称，如镜检、培养、血清学等。可以多个，以“，”分割';
COMMENT ON COLUMN "医院感染记录"."检查方法代码" IS '检查方法在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染记录"."控制方法代码" IS '控制方法在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染记录"."其他侵袭性操作选择器" IS '其他侵袭性操作选择器描述';
COMMENT ON COLUMN "医院感染记录"."住院患者入院科室代码" IS '患者入院科室在原始采集机构编码体系中的代码，住院患者填写入院科室代码';
COMMENT ON COLUMN "医院感染记录"."住院患者入院科室名称" IS '患者入院科室在原始采集机构编码体系中的名称，住院患者填写入院科室名称';
COMMENT ON COLUMN "医院感染记录"."出院时间" IS '患者实际办理出院手续时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院感染记录"."出院诊断代码" IS '按照机构内编码规则赋予出院诊断西医诊断疾病的唯一标识';
COMMENT ON COLUMN "医院感染记录"."出院诊断名称" IS '出院诊断西医诊断在机构内编码体系中的名称';
COMMENT ON COLUMN "医院感染记录"."住院患者出院病室代码" IS '患者出院科室在原始机构编码体系中的代码';
COMMENT ON COLUMN "医院感染记录"."住院患者出院病室名称" IS '患者出院科室在原始机构编码体系中的名称';
COMMENT ON COLUMN "医院感染记录"."感染日期" IS '患者院内感染当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院感染记录"."感染类型代码" IS '感染类型在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染记录"."感染类型名称" IS '感染类型在特定编码体系中的名称，如外源性、内源性';
COMMENT ON COLUMN "医院感染记录"."感染部位代码" IS '感染部位在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染记录"."感染部位名称" IS '感染部位在特定编码体系中的名称，如呼吸系统、手术部位等';
COMMENT ON COLUMN "医院感染记录"."其他感染部位描述" IS '除上述感染部位以外的其他部位描述';
COMMENT ON COLUMN "医院感染记录"."新生儿感染标志" IS '标识是否为新生儿感染';
COMMENT ON COLUMN "医院感染记录"."感染科室代码" IS '患者感染所在科室在机构内编码体系中的代码';
COMMENT ON COLUMN "医院感染记录"."感染科室名称" IS '患者感染所在科室在机构内编码体系中的名称';
COMMENT ON COLUMN "医院感染记录"."感染预后代码" IS '患者感染后治疗结果类别(如治愈、好转、稳定、恶化等)在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染记录"."感染预后" IS '患者感染后治疗结果类别的标准名称，如治愈、好转、稳定、恶化等';
COMMENT ON COLUMN "医院感染记录"."病原学检查标志" IS '标识患者是否进行病原学检查';
COMMENT ON COLUMN "医院感染记录"."医院感染与原发病预后的关系代码" IS '医院感染与原发病预后的关系在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染记录"."医院感染与原发病预后的关系名称" IS '医院感染与原发病预后的关系在特定编码体系中的名称，如无影响、直接原因等';
COMMENT ON COLUMN "医院感染记录"."易感因素代码" IS '易感因素在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染记录"."易感因素名称" IS '易感因素在特定编码体系中的名称，如糖尿病、抗生素、肥胖等。可以多个，用“，”分割';
COMMENT ON COLUMN "医院感染记录"."其他易感因素描述" IS '上述因素以外的其他易感因素描述';
COMMENT ON COLUMN "医院感染记录"."抗生素使用情况代码" IS '抗生素使用情况在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染记录"."抗生素使用情况" IS '抗生素使用情况在特定编码体系中的名称，如一联、二联等';
COMMENT ON COLUMN "医院感染记录"."侵袭性操作选择器代码" IS '侵袭性操作选择器在特定编码体系中的代码';
COMMENT ON TABLE "医院感染记录" IS '患者医院感染疾病类型、部位、日期以及感染预后信息的记录';


CREATE TABLE IF NOT EXISTS "医院分担超指标数据" (
"医疗机构名称" varchar (70) DEFAULT NULL,
 "账套编码" varchar (32) NOT NULL,
 "统计年份" varchar (4) NOT NULL,
 "统计月份" varchar (2) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "总额预付定额" decimal (10,
 2) DEFAULT NULL,
 "报销申报金额" decimal (10,
 2) DEFAULT NULL,
 "超预付金额数" decimal (10,
 2) DEFAULT NULL,
 "医院自承担超标金额" decimal (10,
 2) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医院分担超指标编号" varchar (32) NOT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 CONSTRAINT "医院分担超指标数据"_"账套编码"_"统计年份"_"统计月份"_"医院分担超指标编号"_"医疗机构代码"_PK PRIMARY KEY ("账套编码",
 "统计年份",
 "统计月份",
 "医院分担超指标编号",
 "医疗机构代码")
);
COMMENT ON COLUMN "医院分担超指标数据"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "医院分担超指标数据"."医院分担超指标编号" IS '按照某一特定编码规则赋予医院分担超指标数据记录的顺序号，是医院分担超指标数据记录的唯一标识';
COMMENT ON COLUMN "医院分担超指标数据"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院分担超指标数据"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院分担超指标数据"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "医院分担超指标数据"."医院自承担超标金额" IS '按照医疗保险方与医院达成的协议，超过总额预付金额那一部分按照协议比例医院自身承担的金额，，计量单位为人民币元';
COMMENT ON COLUMN "医院分担超指标数据"."超预付金额数" IS '实际发生的医疗保险费用超过医疗保险方预付的金额，理论上：超预付金额数=报销申报金额-总额预付金额，若不超过，填0，，计量单位为人民币元';
COMMENT ON COLUMN "医院分担超指标数据"."报销申报金额" IS '期间段内医院向医疗保险机构申报的实际发生的医疗保险费用总金额，，计量单位为人民币元';
COMMENT ON COLUMN "医院分担超指标数据"."总额预付定额" IS '医疗保险方与医院达成的一个期间段内付给医院医疗费用报销的总额度，计量单位为人民币元';
COMMENT ON COLUMN "医院分担超指标数据"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "医院分担超指标数据"."统计月份" IS '统计月份的描述，格式为MM';
COMMENT ON COLUMN "医院分担超指标数据"."统计年份" IS '统计年份的描述，格式为YYYY';
COMMENT ON COLUMN "医院分担超指标数据"."账套编码" IS '用于资产管理的唯一性编码';
COMMENT ON COLUMN "医院分担超指标数据"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON TABLE "医院分担超指标数据" IS '医院分担超指标情况，包括指标编号、申报金额、超预付金额、医院自承担超标金额等';


CREATE TABLE IF NOT EXISTS "医疗服务机构" (
"机构类型代码" varchar (4) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "医院简介" varchar (500) DEFAULT NULL,
 "医院评价" varchar (1500) DEFAULT NULL,
 "首页地址" varchar (500) DEFAULT NULL,
 "运营时间" varchar (2000) DEFAULT NULL,
 "交通路线" varchar (3500) DEFAULT NULL,
 "影像路径" varchar (750) DEFAULT NULL,
 "营业范围" varchar (500) DEFAULT NULL,
 "赔付方式" varchar (100) DEFAULT NULL,
 "诊间支付标志" varchar (1) DEFAULT NULL,
 "特殊分类" varchar (200) DEFAULT NULL,
 "统筹区代码" varchar (20) DEFAULT NULL,
 "备注" varchar (1000) DEFAULT NULL,
 "单位财务联系人所在部门" varchar (20) DEFAULT NULL,
 "单位财务联系人电话" varchar (20) DEFAULT NULL,
 "单位财务联系人姓名" varchar (50) DEFAULT NULL,
 "医疗清洁算专管员电话" varchar (20) DEFAULT NULL,
 "医疗清洁算专管员所在部门" varchar (20) DEFAULT NULL,
 "医疗清洁算专管员姓名" varchar (50) DEFAULT NULL,
 "联系电话号码" varchar (20) DEFAULT NULL,
 "纬度" decimal (10,
 2) DEFAULT NULL,
 "经度" decimal (10,
 2) DEFAULT NULL,
 "邮政编码" varchar (6) DEFAULT NULL,
 "机构地址-门牌号码" varchar (70) DEFAULT NULL,
 "机构地址-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "机构地址-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "机构地址-县(市、区)名称" varchar (70) DEFAULT NULL,
 "机构地址-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "机构地址-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "机构地址" varchar (200) DEFAULT NULL,
 "行政区划代码" varchar (12) DEFAULT NULL,
 "联系人姓名" varchar (50) DEFAULT NULL,
 "分管医保负责人姓名" varchar (50) DEFAULT NULL,
 "负责人姓名" varchar (50) DEFAULT NULL,
 "法定代表人电话" varchar (20) DEFAULT NULL,
 "法定代表人证件号码" varchar (50) DEFAULT NULL,
 "法定代表人证件类型代码" varchar (2) DEFAULT NULL,
 "法定代表人姓名" varchar (50) DEFAULT NULL,
 "上级医院名称" varchar (70) DEFAULT NULL,
 "上级医院代码" varchar (50) DEFAULT NULL,
 "分院标志" varchar (1) DEFAULT NULL,
 "编制床位数" decimal (10,
 2) DEFAULT NULL,
 "发证日期" date DEFAULT NULL,
 "发证机关" varchar (100) DEFAULT NULL,
 "有效期到期日期" date DEFAULT NULL,
 "有效期开始日期" date DEFAULT NULL,
 "医疗机构执业范围" varchar (2000) DEFAULT NULL,
 "许可证号码" varchar (40) DEFAULT NULL,
 "主办类型" varchar (1) DEFAULT NULL,
 "医疗机构性质代码" varchar (5) DEFAULT NULL,
 "医疗机构分类管理代码" varchar (30) DEFAULT NULL,
 "医疗机构分类代码" varchar (5) DEFAULT NULL,
 "组织机构代码" varchar (18) DEFAULT NULL,
 "机构等级代码" varchar (2) DEFAULT NULL,
 CONSTRAINT "医疗服务机构"_"医疗机构代码"_PK PRIMARY KEY ("医疗机构代码")
);
COMMENT ON COLUMN "医疗服务机构"."机构等级代码" IS '按照《医院分级管理标准》划分的医院等级在特定编码体系中的代码';
COMMENT ON COLUMN "医疗服务机构"."组织机构代码" IS '医疗服务机构的组织机构代码';
COMMENT ON COLUMN "医疗服务机构"."医疗机构分类代码" IS '由全国组织机构代码管理中心为在中华人民共和国境内对机关、企业、事业单位、社会团体和民办非企业单位按单位所有制和经营方式划分的类别代码';
COMMENT ON COLUMN "医疗服务机构"."医疗机构分类管理代码" IS '机构属于非营利性、营利性或其他医疗机构的分类管理代码';
COMMENT ON COLUMN "医疗服务机构"."医疗机构性质代码" IS '指《医疗机构执业许可证》注明的医疗机构属性代码';
COMMENT ON COLUMN "医疗服务机构"."主办类型" IS '医疗机构的主办类型';
COMMENT ON COLUMN "医疗服务机构"."许可证号码" IS '医疗机构执业许可证登记号';
COMMENT ON COLUMN "医疗服务机构"."医疗机构执业范围" IS '医疗服务机构的执业范围描述';
COMMENT ON COLUMN "医疗服务机构"."有效期开始日期" IS '医疗机构执业许可证的有效期开始日期';
COMMENT ON COLUMN "医疗服务机构"."有效期到期日期" IS '医疗机构执业许可证的有效期到期日期';
COMMENT ON COLUMN "医疗服务机构"."发证机关" IS '医疗机构执业许可证的发证机关';
COMMENT ON COLUMN "医疗服务机构"."发证日期" IS '医疗机构执业许可证的发证日期';
COMMENT ON COLUMN "医疗服务机构"."编制床位数" IS '医疗机构编制床位数';
COMMENT ON COLUMN "医疗服务机构"."分院标志" IS '标识该机构是否为分院';
COMMENT ON COLUMN "医疗服务机构"."上级医院代码" IS '医疗服务机构所属的上级医院代码';
COMMENT ON COLUMN "医疗服务机构"."上级医院名称" IS '医疗服务机构所属的上级医院名称';
COMMENT ON COLUMN "医疗服务机构"."法定代表人姓名" IS '法定代表人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医疗服务机构"."法定代表人证件类型代码" IS '法定代表人身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "医疗服务机构"."法定代表人证件号码" IS '法定代表人各类身份证件上的唯一标识';
COMMENT ON COLUMN "医疗服务机构"."法定代表人电话" IS '法定代表人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "医疗服务机构"."负责人姓名" IS '机构负责人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医疗服务机构"."分管医保负责人姓名" IS '分管医保负责人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医疗服务机构"."联系人姓名" IS '机构联系人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医疗服务机构"."行政区划代码" IS '医疗服务机构所属的行政区划代码';
COMMENT ON COLUMN "医疗服务机构"."机构地址" IS '医疗服务机构所在详细地址描述';
COMMENT ON COLUMN "医疗服务机构"."机构地址-省(自治区、直辖市)名称" IS '机构地址中的省、自治区或直辖市名称';
COMMENT ON COLUMN "医疗服务机构"."机构地址-市(地区、州)名称" IS '机构地址中的市、地区或州的名称';
COMMENT ON COLUMN "医疗服务机构"."机构地址-县(市、区)名称" IS '机构地址中的县（区）的名称';
COMMENT ON COLUMN "医疗服务机构"."机构地址-乡(镇、街道办事处)名称" IS '机构地址中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "医疗服务机构"."机构地址-村(街、路、弄等)名称" IS '机构地址中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "医疗服务机构"."机构地址-门牌号码" IS '机构地址中的门牌号码';
COMMENT ON COLUMN "医疗服务机构"."邮政编码" IS '医疗服务机构所属地区邮政编码';
COMMENT ON COLUMN "医疗服务机构"."经度" IS '医疗服务机构地址经度';
COMMENT ON COLUMN "医疗服务机构"."纬度" IS '医疗服务机构地址纬度';
COMMENT ON COLUMN "医疗服务机构"."联系电话号码" IS '医疗服务机构的电话号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "医疗服务机构"."医疗清洁算专管员姓名" IS '医疗清洁算专管员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医疗服务机构"."医疗清洁算专管员所在部门" IS '医疗清洁算专管员所在部门名称';
COMMENT ON COLUMN "医疗服务机构"."医疗清洁算专管员电话" IS '医疗清洁算专管员的电话号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "医疗服务机构"."单位财务联系人姓名" IS '单位财务联系人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医疗服务机构"."单位财务联系人电话" IS '单位财务联系人的电话号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "医疗服务机构"."单位财务联系人所在部门" IS '单位财务联系人所在部门名称';
COMMENT ON COLUMN "医疗服务机构"."备注" IS '医疗服务机构其他信息的补充说明';
COMMENT ON COLUMN "医疗服务机构"."统筹区代码" IS '医疗服务机构所属的统筹区编码';
COMMENT ON COLUMN "医疗服务机构"."特殊分类" IS '医疗服务机构特殊分类描述';
COMMENT ON COLUMN "医疗服务机构"."诊间支付标志" IS '标识该医疗机构是否支持诊间支付';
COMMENT ON COLUMN "医疗服务机构"."赔付方式" IS '医疗机构赔付方式描述';
COMMENT ON COLUMN "医疗服务机构"."营业范围" IS '医疗服务机构的营业范围的详细描述';
COMMENT ON COLUMN "医疗服务机构"."影像路径" IS '医疗服务机构影像路径描述';
COMMENT ON COLUMN "医疗服务机构"."交通路线" IS '医疗服务机构交通路线的详细描述';
COMMENT ON COLUMN "医疗服务机构"."运营时间" IS '医疗服务机构的运营时间描述';
COMMENT ON COLUMN "医疗服务机构"."首页地址" IS '医疗服务机构首页地址';
COMMENT ON COLUMN "医疗服务机构"."医院评价" IS '医疗服务机构的评价描述';
COMMENT ON COLUMN "医疗服务机构"."医院简介" IS '医疗服务机构整体概况简要描述';
COMMENT ON COLUMN "医疗服务机构"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "医疗服务机构"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医疗服务机构"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医疗服务机构"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "医疗服务机构"."医疗机构代码" IS '按照一定编码规则赋予医疗服务机构的唯一标识';
COMMENT ON COLUMN "医疗服务机构"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "医疗服务机构"."机构类型代码" IS '医疗服务机构类型在特定编码体系中的代码，如医院、卫生院、卫生室等';
COMMENT ON TABLE "医疗服务机构" IS '医疗服务机构的基本信息，包括机构代码、名称、组织机构代码、等级、性质以及营业范围等';


CREATE TABLE IF NOT EXISTS "医生排班和预约信息" (
"修改标志" varchar (1) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医生姓名" varchar (50) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "科室代码" varchar (20) NOT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "医生工号" varchar (20) NOT NULL,
 "门诊地址" varchar (100) NOT NULL,
 "时间段" varchar (8) NOT NULL,
 "门诊类型" varchar (30) NOT NULL,
 "门诊日期" date NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "备注" varchar (1000) DEFAULT NULL,
 "挂号费标准" varchar (6) DEFAULT NULL,
 "已预约号数" varchar (3) DEFAULT NULL,
 "已挂号数" varchar (3) DEFAULT NULL,
 "限预约号数" decimal (3,
 0) DEFAULT NULL,
 "限号数" decimal (5,
 0) DEFAULT NULL,
 CONSTRAINT "医生排班和预约信息"_"医疗机构代码"_"科室代码"_"医生工号"_"门诊地址"_"时间段"_"门诊类型"_"门诊日期"_PK PRIMARY KEY ("医疗机构代码",
 "科室代码",
 "医生工号",
 "门诊地址",
 "时间段",
 "门诊类型",
 "门诊日期")
);
COMMENT ON COLUMN "医生排班和预约信息"."限号数" IS '医生该时间段的最大挂号数量';
COMMENT ON COLUMN "医生排班和预约信息"."限预约号数" IS '医生该时间段的最大预约数据量';
COMMENT ON COLUMN "医生排班和预约信息"."已挂号数" IS '医生在该时间段已经挂号人数';
COMMENT ON COLUMN "医生排班和预约信息"."已预约号数" IS '医生在该时间段已经预约挂号人数';
COMMENT ON COLUMN "医生排班和预约信息"."挂号费标准" IS '医生的挂号费标准描述';
COMMENT ON COLUMN "医生排班和预约信息"."备注" IS '关于医生排班和预约信息的其他补充说明';
COMMENT ON COLUMN "医生排班和预约信息"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "医生排班和预约信息"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医生排班和预约信息"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医生排班和预约信息"."门诊日期" IS '医生门诊当日的公元纪年日期的详细描述';
COMMENT ON COLUMN "医生排班和预约信息"."门诊类型" IS '医生门诊类型的详细描述';
COMMENT ON COLUMN "医生排班和预约信息"."时间段" IS '医生坐诊时间段描述，如8:00-10:00';
COMMENT ON COLUMN "医生排班和预约信息"."门诊地址" IS '医生门诊地址的详细描述';
COMMENT ON COLUMN "医生排班和预约信息"."医生工号" IS '医生在特定编码体系中的顺序号';
COMMENT ON COLUMN "医生排班和预约信息"."科室名称" IS '医生所在科室在特定编码体系中的名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "医生排班和预约信息"."科室代码" IS '按照特定编码规则赋予医生所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "医生排班和预约信息"."医疗机构代码" IS '按照某一特定编码规则赋予医疗机构的唯一标识';
COMMENT ON COLUMN "医生排班和预约信息"."医生姓名" IS '医生在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医生排班和预约信息"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "医生排班和预约信息"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON TABLE "医生排班和预约信息" IS '医生每日每个时间段的排班和预约信息，包括门诊日期、时间段、限定挂号数、已挂号数等';


CREATE TABLE IF NOT EXISTS "医护人员信息表" (
"医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "职工代码" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "职工工号" varchar (20) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "民族代码" varchar (2) DEFAULT NULL,
 "民族名称" varchar (50) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "所属科室代码" varchar (20) DEFAULT NULL,
 "所属科室名称" varchar (100) DEFAULT NULL,
 "所属中心科室代码" varchar (20) DEFAULT NULL,
 "平台科室类别" varchar (1) DEFAULT NULL,
 "职务代码" varchar (32) DEFAULT NULL,
 "职务名称" varchar (32) DEFAULT NULL,
 "职称代码" varchar (32) DEFAULT NULL,
 "职称名称" varchar (32) DEFAULT NULL,
 "专业代码" varchar (32) DEFAULT NULL,
 "专业名称" varchar (20) DEFAULT NULL,
 "人员专业类别代码" varchar (2) DEFAULT NULL,
 "邮箱地址" varchar (32) DEFAULT NULL,
 "家庭地址" varchar (200) DEFAULT NULL,
 "邮政编码" varchar (6) DEFAULT NULL,
 "电话号码" varchar (32) DEFAULT NULL,
 "手机号码" varchar (20) DEFAULT NULL,
 "医生简介" varchar (4000) DEFAULT NULL,
 "专科特长" varchar (300) DEFAULT NULL,
 "可挂号标志" varchar (1) DEFAULT NULL,
 "开立处方权限类别代码" varchar (50) DEFAULT NULL,
 "学历代码" varchar (5) DEFAULT NULL,
 "学位代码" varchar (1) DEFAULT NULL,
 "行政管理职务代码" varchar (1) DEFAULT NULL,
 "行政管理职务名称" varchar (64) DEFAULT NULL,
 "岗位名称" varchar (64) DEFAULT NULL,
 "执业证书号" varchar (64) DEFAULT NULL,
 "执业资格代码" varchar (50) DEFAULT NULL,
 "执业资格名称" varchar (64) DEFAULT NULL,
 "执业证书注册日期" date DEFAULT NULL,
 "全科医生标志" varchar (1) DEFAULT NULL,
 "医师执业类别代码" varchar (1) DEFAULT NULL,
 "医师执业范围代码" varchar (30) DEFAULT NULL,
 "多地点执业医师标志" varchar (1) DEFAULT NULL,
 "第2执业单位类别代码" varchar (1) DEFAULT NULL,
 "第3执业单位类别代码" varchar (1) DEFAULT NULL,
 "乡镇卫生院或社区卫生服务机构派驻工作标志" varchar (1) DEFAULT NULL,
 "中国科学院士和中国工程院院士标志" varchar (1) DEFAULT NULL,
 "突出贡献青年科学、技术、管理专家标志" varchar (1) DEFAULT NULL,
 "享受国务院政府特殊津贴人员标志" varchar (1) DEFAULT NULL,
 "新世纪百千万人才工程国家级人选标志" varchar (1) DEFAULT NULL,
 "国家科技奖项负责人标志" varchar (1) DEFAULT NULL,
 "人员调入情况" varchar (2) DEFAULT NULL,
 "人员调出情况" varchar (2) DEFAULT NULL,
 "调入时间" timestamp DEFAULT NULL,
 "调出时间" timestamp DEFAULT NULL,
 "参加工作日期" varchar (6) DEFAULT NULL,
 "编制情况" varchar (1) DEFAULT NULL,
 "工作状态" varchar (2) DEFAULT NULL,
 "培训类别" varchar (2) DEFAULT NULL,
 "试用期标志" varchar (1) DEFAULT NULL,
 "政治面貌代码" varchar (2) DEFAULT NULL,
 "岗位级别" varchar (2) DEFAULT NULL,
 "纳入全科医生培训标志" varchar (1) DEFAULT NULL,
 "全科医生注册证书号" varchar (50) DEFAULT NULL,
 "全科医生注册证书日期" date DEFAULT NULL,
 "注册证书号" varchar (50) DEFAULT NULL,
 "注册时间" timestamp DEFAULT NULL,
 "聘用标志" varchar (1) DEFAULT NULL,
 "现聘岗位时间" timestamp DEFAULT NULL,
 "调出地点代码" varchar (50) DEFAULT NULL,
 "调出地点名称" varchar (100) DEFAULT NULL,
 "借出时间" timestamp DEFAULT NULL,
 "借出单位代码" varchar (50) DEFAULT NULL,
 "借出单位名称" varchar (100) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "医护人员信息表"_"医疗机构代码"_"职工代码"_PK PRIMARY KEY ("医疗机构代码",
 "职工代码")
);
COMMENT ON COLUMN "医护人员信息表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医护人员信息表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医护人员信息表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "医护人员信息表"."借出单位名称" IS '员工借出单位的名称';
COMMENT ON COLUMN "医护人员信息表"."借出单位代码" IS '按照某一特定编码规则赋予借出单位唯一标识';
COMMENT ON COLUMN "医护人员信息表"."借出时间" IS '员工借出的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医护人员信息表"."调出地点名称" IS '员工调出地点的详细描述';
COMMENT ON COLUMN "医护人员信息表"."调出地点代码" IS '员工调查地点在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."现聘岗位时间" IS '员工现聘岗位开始的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医护人员信息表"."聘用标志" IS '标识员工是否被聘用的标志';
COMMENT ON COLUMN "医护人员信息表"."注册时间" IS '注册证书号注册当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医护人员信息表"."注册证书号" IS '注册证书的编码';
COMMENT ON COLUMN "医护人员信息表"."全科医生注册证书日期" IS '全科医师证书注册时的公元纪年日期';
COMMENT ON COLUMN "医护人员信息表"."全科医生注册证书号" IS '全科医师注册证书的编码';
COMMENT ON COLUMN "医护人员信息表"."纳入全科医生培训标志" IS '表示卫生人员是否纳入全科医师培训的标志';
COMMENT ON COLUMN "医护人员信息表"."岗位级别" IS '员工聘用岗位对应的级别';
COMMENT ON COLUMN "医护人员信息表"."政治面貌代码" IS '所属党派类别在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."试用期标志" IS '标识员工是否处于试用期的标志';
COMMENT ON COLUMN "医护人员信息表"."培训类别" IS '培训类别的名称，如1：住院医师培训；2：全科医师培训';
COMMENT ON COLUMN "医护人员信息表"."工作状态" IS '工作状态(如1：在岗、2：停薪留职、3：长病假、4：借出外系统)的详细描述';
COMMENT ON COLUMN "医护人员信息表"."编制情况" IS '编制情况(如1编制内；2合同制；3临聘人员；4返聘)的详细描述';
COMMENT ON COLUMN "医护人员信息表"."参加工作日期" IS '开始从事某职业首日的公元纪年日期，格式为YYYYMM';
COMMENT ON COLUMN "医护人员信息表"."调出时间" IS '人员调出时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医护人员信息表"."调入时间" IS '人员调入时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医护人员信息表"."人员调出情况" IS '人员调出情况(如22考取研究生；23出国留学；24退休；25辞职；26自然减员；27辞退；29 其他；31调往本市本系统；32调往本市外系统；33调往外市本系统；34调往外市外系统)的详细描述';
COMMENT ON COLUMN "医护人员信息表"."人员调入情况" IS '人员调入情况(如11高中等院校毕业生；12其他卫生机构调入；13非卫生机构调入；14军转人员；19其他)的详细描述';
COMMENT ON COLUMN "医护人员信息表"."国家科技奖项负责人标志" IS '职工是国家科技奖项负责人的标识';
COMMENT ON COLUMN "医护人员信息表"."新世纪百千万人才工程国家级人选标志" IS '职工是新世纪百千万人才工程国家级人选的标识';
COMMENT ON COLUMN "医护人员信息表"."享受国务院政府特殊津贴人员标志" IS '职工是享受国务院政府特殊津贴人员的标识';
COMMENT ON COLUMN "医护人员信息表"."突出贡献青年科学、技术、管理专家标志" IS '职工是有突出贡献的中青年科学、技术、管理专家的标识';
COMMENT ON COLUMN "医护人员信息表"."中国科学院士和中国工程院院士标志" IS '职工是中国科学院士和中国工程院院士的标识';
COMMENT ON COLUMN "医护人员信息表"."乡镇卫生院或社区卫生服务机构派驻工作标志" IS '职工由乡镇卫生院或社区卫生服务机构派驻村卫生室工作的标识';
COMMENT ON COLUMN "医护人员信息表"."第3执业单位类别代码" IS '第3执业单位类别(如医院、乡镇卫生院、社区卫生服务中心等)在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."第2执业单位类别代码" IS '第2执业单位类别(如医院、乡镇卫生院、社区卫生服务中心等)在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."多地点执业医师标志" IS '标识卫生人员是否注册为多机构执业医师的标志';
COMMENT ON COLUMN "医护人员信息表"."医师执业范围代码" IS '医师执业科室范围(如内科专业、外科专业、妇产科专业等)在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."医师执业类别代码" IS '医师职业类别(如临床、口腔、公卫等)在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."全科医生标志" IS '标识是否为全科医师的标志';
COMMENT ON COLUMN "医护人员信息表"."执业证书注册日期" IS '执业资格证书注册时的公元纪年日期';
COMMENT ON COLUMN "医护人员信息表"."执业资格名称" IS '执业资格名称，如医师资格证、药剂师资格证、助产师证、心理咨询师证等';
COMMENT ON COLUMN "医护人员信息表"."执业资格代码" IS '执业资格在编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."执业证书号" IS '执业证书的编码';
COMMENT ON COLUMN "医护人员信息表"."岗位名称" IS '员工聘用岗位的名称';
COMMENT ON COLUMN "医护人员信息表"."行政管理职务名称" IS '标识个体行政职务级别标准名称，如如党委(副)书记、院(所/站)长、科室主任';
COMMENT ON COLUMN "医护人员信息表"."行政管理职务代码" IS '标识个体行政管理的职务级别(如党委(副)书记、院(所/站)长、科室主任等)在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."学位代码" IS '获得的学位(学士学位、硕士学位、博士学位)类别在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."学历代码" IS '个体受教育最高程度的类别(如研究生教育、大学本科、专科教育等)在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."开立处方权限类别代码" IS '开立处方类别的权限类别在编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."可挂号标志" IS '标识该卫生人员是否可以挂号的标志';
COMMENT ON COLUMN "医护人员信息表"."专科特长" IS '职工擅长的专业方向介绍';
COMMENT ON COLUMN "医护人员信息表"."医生简介" IS '职工简要介绍说明';
COMMENT ON COLUMN "医护人员信息表"."手机号码" IS '居民本人电话号码，含区号和分机号';
COMMENT ON COLUMN "医护人员信息表"."电话号码" IS '本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "医护人员信息表"."邮政编码" IS '家庭地址所在行政区的邮政编码';
COMMENT ON COLUMN "医护人员信息表"."家庭地址" IS '医护人员家庭地址的详细描述';
COMMENT ON COLUMN "医护人员信息表"."邮箱地址" IS '医护人员的电子邮箱名称';
COMMENT ON COLUMN "医护人员信息表"."人员专业类别代码" IS '从事专业类别(如执业医师、注册护士、助产士、医药师等)在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."专业名称" IS '最高学历所学专业在特定编码体系中的名称';
COMMENT ON COLUMN "医护人员信息表"."专业代码" IS '最高学历所学专业在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."职称名称" IS '职工的职称级别在特定编码体系中的名称，如正高、副高等';
COMMENT ON COLUMN "医护人员信息表"."职称代码" IS '职工的职称级别在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."职务名称" IS '职工的专业技术职务在特定编码体系中的名称，如主任医师、主治医师等';
COMMENT ON COLUMN "医护人员信息表"."职务代码" IS '职工的专业技术职务在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."平台科室类别" IS '科室业务类别的详细描述，A医疗机构诊疗科目名录，B疾病预防控制中心业务科室分类与代码，C卫生监督机构业务科室分类与代码，D卫生机构管理科室分类与代码';
COMMENT ON COLUMN "医护人员信息表"."所属中心科室代码" IS '医护人员所属科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."所属科室名称" IS '医护人员所属科室在机构内编码体系中的名称';
COMMENT ON COLUMN "医护人员信息表"."所属科室代码" IS '医护人员所属科室在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "医护人员信息表"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "医护人员信息表"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."民族名称" IS '所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "医护人员信息表"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "医护人员信息表"."性别名称" IS '个体生理性别在特定编码体系中的名称';
COMMENT ON COLUMN "医护人员信息表"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "医护人员信息表"."姓名" IS '员工在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医护人员信息表"."职工工号" IS '医护人员工号';
COMMENT ON COLUMN "医护人员信息表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "医护人员信息表"."职工代码" IS '医护人员在his系统内部的唯一标识';
COMMENT ON COLUMN "医护人员信息表"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "医护人员信息表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON TABLE "医护人员信息表" IS '医疗机构内卫生人员的个人基本信息以及职业相关信息';


CREATE TABLE IF NOT EXISTS "剖宫产手术记录" (
"术后出血量(mL)" decimal (5,
 0) DEFAULT NULL,
 "术后心率(次/min)" decimal (3,
 0) DEFAULT NULL,
 "术后脉搏(次/min)" decimal (3,
 0) DEFAULT NULL,
 "术后舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "术后收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "术后检查时间(min)" decimal (3,
 0) DEFAULT NULL,
 "术后观察时间" timestamp DEFAULT NULL,
 "术后诊断" varchar (200) DEFAULT NULL,
 "手术全程时长(min)" decimal (4,
 0) DEFAULT NULL,
 "手术结束时间" timestamp DEFAULT NULL,
 "其他用药情况" varchar (100) DEFAULT NULL,
 "其他用药描述" varchar (50) DEFAULT NULL,
 "供氧时长(min)" decimal (4,
 0) DEFAULT NULL,
 "输液量(mL)" decimal (5,
 0) DEFAULT NULL,
 "输血量(自体回收)" decimal (6,
 0) DEFAULT NULL,
 "输血量(全血)" decimal (6,
 0) DEFAULT NULL,
 "输血量(血浆)" decimal (6,
 0) DEFAULT NULL,
 "输血量(血小板)" decimal (6,
 0) DEFAULT NULL,
 "输血量(红细胞)" decimal (6,
 0) DEFAULT NULL,
 "输血量(mL)" decimal (4,
 0) DEFAULT NULL,
 "输血成分" varchar (100) DEFAULT NULL,
 "出血量(mL)" decimal (5,
 0) DEFAULT NULL,
 "手术时产妇情况" varchar (100) DEFAULT NULL,
 "宫腔探查处理情况" varchar (100) DEFAULT NULL,
 "宫腔探查异常情况" varchar (100) DEFAULT NULL,
 "腹腔探查附件" varchar (100) DEFAULT NULL,
 "腹腔探查子宫" varchar (100) DEFAULT NULL,
 "手术用药量" varchar (50) DEFAULT NULL,
 "手术用药" varchar (50) DEFAULT NULL,
 "宫缩剂使用方法" varchar (100) DEFAULT NULL,
 "宫缩剂名称" varchar (50) DEFAULT NULL,
 "子宫壁缝合情况" varchar (100) DEFAULT NULL,
 "存脐带血情况标志" varchar (1) DEFAULT NULL,
 "脐带异常情况描述" varchar (200) DEFAULT NULL,
 "脐带长度(cm)" decimal (5,
 0) DEFAULT NULL,
 "绕颈身(周)" decimal (3,
 0) DEFAULT NULL,
 "胎膜完整情况标志" varchar (1) DEFAULT NULL,
 "胎盘娩出情况" varchar (100) DEFAULT NULL,
 "胎盘娩出时间" timestamp DEFAULT NULL,
 "羊水量" decimal (5,
 0) DEFAULT NULL,
 "羊水性状" varchar (100) DEFAULT NULL,
 "胎儿娩出时间" timestamp DEFAULT NULL,
 "胎儿娩出方式" varchar (100) DEFAULT NULL,
 "子宫情况" varchar (100) DEFAULT NULL,
 "剖宫产手术过程" varchar (200) DEFAULT NULL,
 "麻醉效果" varchar (100) DEFAULT NULL,
 "麻醉体位" varchar (100) DEFAULT NULL,
 "麻醉方法代码" varchar (20) DEFAULT NULL,
 "手术开始时间" timestamp DEFAULT NULL,
 "手术及操作代码" varchar (20) DEFAULT NULL,
 "手术指征" varchar (500) DEFAULT NULL,
 "术前诊断" varchar (200) DEFAULT NULL,
 "待产时间" timestamp DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "产妇姓名" varchar (50) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "待产记录流水号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "剖宫产手术记录流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "记录人工号" varchar (20) DEFAULT NULL,
 "记录人姓名" varchar (50) DEFAULT NULL,
 "儿科医生工号" varchar (20) DEFAULT NULL,
 "儿科医生姓名" varchar (50) DEFAULT NULL,
 "手术助手工号" varchar (20) DEFAULT NULL,
 "手术助手姓名" varchar (50) DEFAULT NULL,
 "器械护士工号" varchar (20) DEFAULT NULL,
 "器械护士姓名" varchar (50) DEFAULT NULL,
 "麻醉医生工号" varchar (20) DEFAULT NULL,
 "麻醉医生姓名" varchar (50) DEFAULT NULL,
 "手术医生工号" varchar (20) DEFAULT NULL,
 "手术医生姓名" varchar (50) DEFAULT NULL,
 "新生儿异常情况代码" varchar (1) DEFAULT NULL,
 "分娩结局代码" varchar (1) DEFAULT NULL,
 "Apgar评分值" decimal (2,
 0) DEFAULT NULL,
 "Apgar评分间隔时间代码" varchar (1) DEFAULT NULL,
 "产瘤部位" varchar (100) DEFAULT NULL,
 "产瘤大小" varchar (100) DEFAULT NULL,
 "新生儿出生身长(cm)" decimal (5,
 1) DEFAULT NULL,
 "新生儿出生体重(g)" decimal (4,
 0) DEFAULT NULL,
 "新生儿性别代码" varchar (2) DEFAULT NULL,
 "术后宫底高度(cm)" decimal (4,
 1) DEFAULT NULL,
 "术后宫缩" varchar (200) DEFAULT NULL,
 CONSTRAINT "剖宫产手术记录"_"剖宫产手术记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("剖宫产手术记录流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "剖宫产手术记录"."术后宫缩" IS '产妇术后宫缩强度等情况的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."术后宫底高度(cm)" IS '产妇产后耻骨联合上缘至子宫底部距离的测量值，计量单位为 cm';
COMMENT ON COLUMN "剖宫产手术记录"."新生儿性别代码" IS '新生儿生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "剖宫产手术记录"."新生儿出生体重(g)" IS '新生儿出生后第1小时内第1次称得的重量，计量单位为g';
COMMENT ON COLUMN "剖宫产手术记录"."新生儿出生身长(cm)" IS '新生儿出生卧位身高的测量值,计量单位为cm';
COMMENT ON COLUMN "剖宫产手术记录"."产瘤大小" IS '产瘤大小的详细描述，计量为单位';
COMMENT ON COLUMN "剖宫产手术记录"."产瘤部位" IS '产瘤部位的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."Apgar评分间隔时间代码" IS 'Apgar评分间隔时间(如1分钟、5分钟、10分钟)在特定编码体系中的代码';
COMMENT ON COLUMN "剖宫产手术记录"."Apgar评分值" IS '对新生儿娩出后呼吸、心率、皮肤颜色、肌张力及对刺激的反应等5 项指标的评分结果值，计量单位为分';
COMMENT ON COLUMN "剖宫产手术记录"."分娩结局代码" IS '孕产妇妊娠分娩最终结局(如活产、死胎、死产等)在特定编码体系中的代码';
COMMENT ON COLUMN "剖宫产手术记录"."新生儿异常情况代码" IS '新生儿异常情况类别(如无、早期新生儿死亡、畸形、早产等)在特定编码体系中的代码';
COMMENT ON COLUMN "剖宫产手术记录"."手术医生姓名" IS '实施手术的主要执行人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "剖宫产手术记录"."手术医生工号" IS '实施手术的主要执行人员在原始特定编码体系中的编号';
COMMENT ON COLUMN "剖宫产手术记录"."麻醉医生姓名" IS '对患者实施麻醉的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "剖宫产手术记录"."麻醉医生工号" IS '麻醉医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "剖宫产手术记录"."器械护士姓名" IS '器械护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "剖宫产手术记录"."器械护士工号" IS '器械护士在原始特定编码体系中的编号';
COMMENT ON COLUMN "剖宫产手术记录"."手术助手姓名" IS '协助手术者完成手术及操作的助手在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "剖宫产手术记录"."手术助手工号" IS '协助手术者完成手术及操作的助手在原始特定编码体系中的编号';
COMMENT ON COLUMN "剖宫产手术记录"."儿科医生姓名" IS '儿科医师在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "剖宫产手术记录"."儿科医生工号" IS '儿科医师在原始特定编码体系中的编号';
COMMENT ON COLUMN "剖宫产手术记录"."记录人姓名" IS '记录人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "剖宫产手术记录"."记录人工号" IS '记录人员在机构内特定编码体系中的编号';
COMMENT ON COLUMN "剖宫产手术记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "剖宫产手术记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "剖宫产手术记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "剖宫产手术记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "剖宫产手术记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "剖宫产手术记录"."剖宫产手术记录流水号" IS '按照某一特定编码规则赋予剖宫产手术记录的顺序号';
COMMENT ON COLUMN "剖宫产手术记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "剖宫产手术记录"."待产记录流水号" IS '按照某一特定编码规则赋予待产记录的顺序号，是待产记录的唯一标识';
COMMENT ON COLUMN "剖宫产手术记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "剖宫产手术记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "剖宫产手术记录"."住院次数" IS '表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "剖宫产手术记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "剖宫产手术记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "剖宫产手术记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "剖宫产手术记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "剖宫产手术记录"."产妇姓名" IS '产妇本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "剖宫产手术记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "剖宫产手术记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "剖宫产手术记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "剖宫产手术记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "剖宫产手术记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "剖宫产手术记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "剖宫产手术记录"."待产时间" IS '产妇进入产房时的公元纪年日期和吋间的完整描述';
COMMENT ON COLUMN "剖宫产手术记录"."术前诊断" IS '术前诊断的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."手术指征" IS '患者具备的、适宜实施手术的主要症状和体征描述';
COMMENT ON COLUMN "剖宫产手术记录"."手术及操作代码" IS '手术及操作在特定编码体系中的唯一标识';
COMMENT ON COLUMN "剖宫产手术记录"."手术开始时间" IS '以手术操作刀碰皮(切皮)时点计算，经自然腔道到达手术部位，没有皮肤切口的手术，开始时间以手术器械进入人体腔道的时点为准,对患者开始手术操作时的公元纪年日期和时间的完整描述。';
COMMENT ON COLUMN "剖宫产手术记录"."麻醉方法代码" IS '为患者进行手术、操作时使用的麻醉方法在特定编码体系中的代码';
COMMENT ON COLUMN "剖宫产手术记录"."麻醉体位" IS '麻醉体位的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."麻醉效果" IS '实施麻醉效果的描述';
COMMENT ON COLUMN "剖宫产手术记录"."剖宫产手术过程" IS '剖宫产手术 过程的详细 描述，如腹壁脂肪层厚度、腹膜分离情 况、腹腔粘连枪口、腹水情况、腹壁缝 合、缝合膀胱腹膜反折情况等';
COMMENT ON COLUMN "剖宫产手术记录"."子宫情况" IS '子宫情况的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."胎儿娩出方式" IS '胎儿娩出方式的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."胎儿娩出时间" IS '胎儿娩出时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "剖宫产手术记录"."羊水性状" IS '羊水性状的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."羊水量" IS '羊水量的描述，单位为mL';
COMMENT ON COLUMN "剖宫产手术记录"."胎盘娩出时间" IS '胎盘娩出时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "剖宫产手术记录"."胎盘娩出情况" IS '对胎盘娩出情况的描述,如娩出方式、胎盘重量、胎盘大小、胎盘完整情况、胎盘附着位置等';
COMMENT ON COLUMN "剖宫产手术记录"."胎膜完整情况标志" IS '标识胎膜是否完盤的标志';
COMMENT ON COLUMN "剖宫产手术记录"."绕颈身(周)" IS '脐带绕颈身的周数，计诋单位为周';
COMMENT ON COLUMN "剖宫产手术记录"."脐带长度(cm)" IS '脐带的长度值，计量单位为cm';
COMMENT ON COLUMN "剖宫产手术记录"."脐带异常情况描述" IS '标识脐带是否存在异常情况的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."存脐带血情况标志" IS '标识是否留存脐带血的标志';
COMMENT ON COLUMN "剖宫产手术记录"."子宫壁缝合情况" IS '产妇子宫壁缝合情况的详细描述，如子宫壁缝合层数、缝合线、缝合方法等';
COMMENT ON COLUMN "剖宫产手术记录"."宫缩剂名称" IS '平台中心所使用宫缩剂药物名称';
COMMENT ON COLUMN "剖宫产手术记录"."宫缩剂使用方法" IS '宫缩剂使用方法的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."手术用药" IS '平台中心手术用药药物名称';
COMMENT ON COLUMN "剖宫产手术记录"."手术用药量" IS '手术用药用量的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."腹腔探查子宫" IS '腹腔探査时子宫情况的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."腹腔探查附件" IS '腹腔探查时附件情况的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."宫腔探查异常情况" IS '标识宫腔探查异常情况的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."宫腔探查处理情况" IS '宫腔探查后处理方式的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."手术时产妇情况" IS '手术时产妇情况的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."出血量(mL)" IS '手术中出血 量的累计值， 计量单位为mL';
COMMENT ON COLUMN "剖宫产手术记录"."输血成分" IS '输血成分的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."输血量(mL)" IS '输入红细胞、血小板、血浆、全血等的数量，剂量单位为mL';
COMMENT ON COLUMN "剖宫产手术记录"."输血量(红细胞)" IS '输入红细胞的数量，单位为单位';
COMMENT ON COLUMN "剖宫产手术记录"."输血量(血小板)" IS '输入血小板的数量，单位为袋';
COMMENT ON COLUMN "剖宫产手术记录"."输血量(血浆)" IS '输入血浆的数量，单位为mL';
COMMENT ON COLUMN "剖宫产手术记录"."输血量(全血)" IS '输入全血的数量，单位为mL';
COMMENT ON COLUMN "剖宫产手术记录"."输血量(自体回收)" IS '自体回收的数量，单位为mL';
COMMENT ON COLUMN "剖宫产手术记录"."输液量(mL)" IS '总输液量，单位ml';
COMMENT ON COLUMN "剖宫产手术记录"."供氧时长(min)" IS '供氧时间的时长，计量单位为 min';
COMMENT ON COLUMN "剖宫产手术记录"."其他用药描述" IS '其他使用药物的通用名称';
COMMENT ON COLUMN "剖宫产手术记录"."其他用药情况" IS '其他用药情况的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."手术结束时间" IS '对患者结束手术操作时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "剖宫产手术记录"."手术全程时长(min)" IS '手术全程所 用的总时长， 计量单位为min';
COMMENT ON COLUMN "剖宫产手术记录"."术后诊断" IS '对产妇术后诊断(包括孕产次数)的详细描述';
COMMENT ON COLUMN "剖宫产手术记录"."术后观察时间" IS '术后观察结束时间的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "剖宫产手术记录"."术后检查时间(min)" IS '术后检查时， 距离分娩结 朿后的时间， 计量单位为min';
COMMENT ON COLUMN "剖宫产手术记录"."术后收缩压(mmHg)" IS '术后收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "剖宫产手术记录"."术后舒张压(mmHg)" IS '术后舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "剖宫产手术记录"."术后脉搏(次/min)" IS '术后每分钟脉搏的次数测量值，计量单位为次/min';
COMMENT ON COLUMN "剖宫产手术记录"."术后心率(次/min)" IS '术后心脏搏动频率的测量值，计量单位为次/min';
COMMENT ON COLUMN "剖宫产手术记录"."术后出血量(mL)" IS '产妇术后出血量的累计值，计量单位为 mL';
COMMENT ON TABLE "剖宫产手术记录" IS '剖宫产产时记录，包括手术相关信息以及胎盘胎膜，新生儿评分信息';


CREATE TABLE IF NOT EXISTS "出院登记信息" (
"住院就诊流水号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "新冠感染情况名称" varchar (10) DEFAULT NULL,
 "新冠感染情况代码" varchar (1) DEFAULT NULL,
 "作废标志" varchar (1) DEFAULT NULL,
 "医保卡号" varchar (64) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "保险类型代码" varchar (2) DEFAULT NULL,
 "特需标志" varchar (1) DEFAULT NULL,
 "出院时间" timestamp DEFAULT NULL,
 "出院病区名称" varchar (100) DEFAULT NULL,
 "出院病区代码" varchar (20) DEFAULT NULL,
 "出院科室名称" varchar (100) DEFAULT NULL,
 "出院科室代码" varchar (20) DEFAULT NULL,
 "诊断名称" varchar (100) DEFAULT NULL,
 "诊断代码" varchar (64) DEFAULT NULL,
 "疾病诊断代码类型" varchar (2) DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "留观标志" varchar (1) DEFAULT NULL,
 "入院病区名称" varchar (100) DEFAULT NULL,
 "入院病区代码" varchar (20) DEFAULT NULL,
 "入院科室名称" varchar (100) DEFAULT NULL,
 "入院科室代码" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "常住户籍类型代码" varchar (1) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 CONSTRAINT "出院登记信息"_"住院就诊流水号"_"医疗机构代码"_PK PRIMARY KEY ("住院就诊流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "出院登记信息"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "出院登记信息"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "出院登记信息"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "出院登记信息"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "出院登记信息"."证件类型代码" IS '患者身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "出院登记信息"."证件号码" IS '患者各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "出院登记信息"."姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "出院登记信息"."常住户籍类型代码" IS '患者是否是外地(如本市、外地、境外(港澳台)、外国、未知等)在特定编码体系中的代码';
COMMENT ON COLUMN "出院登记信息"."出生日期" IS '患者出生当日的公元纪年日期';
COMMENT ON COLUMN "出院登记信息"."入院科室代码" IS '患者入院科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "出院登记信息"."入院科室名称" IS '患者入院科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "出院登记信息"."入院病区代码" IS '患者入院时所住病区在机构内编码体系中的代码';
COMMENT ON COLUMN "出院登记信息"."入院病区名称" IS '患者入院时所住病区在机构内编码体系中的名称';
COMMENT ON COLUMN "出院登记信息"."留观标志" IS '标识是否对患者采取留院观察的标志';
COMMENT ON COLUMN "出院登记信息"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "出院登记信息"."疾病诊断代码类型" IS '疾病诊断编码类别(如ICD10、中医国标-95、中医国标-97等)在特定编码体系中的代码';
COMMENT ON COLUMN "出院登记信息"."诊断代码" IS '患者主要诊断在特定编码体系中的代码，包含中医诊断和西医诊断';
COMMENT ON COLUMN "出院登记信息"."诊断名称" IS '患者主要诊断在特定编码体系中的名称，包含中医诊断和西医诊断';
COMMENT ON COLUMN "出院登记信息"."出院科室代码" IS '出院科室在机构内编码体系中的代码';
COMMENT ON COLUMN "出院登记信息"."出院科室名称" IS '出院科室在机构内编码体系中的名称';
COMMENT ON COLUMN "出院登记信息"."出院病区代码" IS '患者出院时所住病区在机构内编码体系中的代码';
COMMENT ON COLUMN "出院登记信息"."出院病区名称" IS '患者出院时所住病区在机构内编码体系中的名称';
COMMENT ON COLUMN "出院登记信息"."出院时间" IS '患者实际办理出院手续时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "出院登记信息"."特需标志" IS '标识患者是否特需患者的标志，其中：0.非特需；1.特需';
COMMENT ON COLUMN "出院登记信息"."保险类型代码" IS '患者参加的医疗保险类别(如城镇职工基本医疗保险、城镇居民基本医疗保险等)在特定编码体系中的代码';
COMMENT ON COLUMN "出院登记信息"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "出院登记信息"."医保卡号" IS '参合人所参加的医疗保险使用卡的编号，即医保局确定一个医保参保人员的唯一编号(卡号一般为16位数)';
COMMENT ON COLUMN "出院登记信息"."作废标志" IS '标识是否作废';
COMMENT ON COLUMN "出院登记信息"."新冠感染情况代码" IS '新冠感染情况(如未查/不详、阴性、阳性等)在特定编码体系中的代码';
COMMENT ON COLUMN "出院登记信息"."新冠感染情况名称" IS '新冠感染情况(如未查/不详、阴性、阳性等)在特定编码体系中的名称';
COMMENT ON COLUMN "出院登记信息"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "出院登记信息"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "出院登记信息"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "出院登记信息"."性别代码" IS '患者生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "出院登记信息"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "出院登记信息"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "出院登记信息"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON TABLE "出院登记信息" IS '病人确认出院后，提供的出院登记信息，包括入出院科室、出院时间数据';


CREATE TABLE IF NOT EXISTS "其他知情同意书" (
"知情同意书名称" varchar (200) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "疾病诊断名称" varchar (512) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "医师签名时间" timestamp DEFAULT NULL,
 "医师姓名" varchar (50) DEFAULT NULL,
 "医师工号" varchar (20) DEFAULT NULL,
 "患者/法定代理人签名时间" timestamp DEFAULT NULL,
 "法定代理人与患者的关系名称" varchar (100) DEFAULT NULL,
 "法定代理人与患者的关系代码" varchar (1) DEFAULT NULL,
 "法定代理人姓名" varchar (50) DEFAULT NULL,
 "患者签名" varchar (50) DEFAULT NULL,
 "患者/法定代理人意见" text,
 "医疗机构意见" text,
 "知情同意内容" text,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "其他知情同意书流水号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "门诊就诊流水号" varchar (32) DEFAULT NULL,
 "知情同意书编号" varchar (20) DEFAULT NULL,
 CONSTRAINT "其他知情同意书"_"医疗机构代码"_"其他知情同意书流水号"_PK PRIMARY KEY ("医疗机构代码",
 "其他知情同意书流水号")
);
COMMENT ON COLUMN "其他知情同意书"."知情同意书编号" IS '按照某一特定编码规则赋予知情同意书的唯一标识';
COMMENT ON COLUMN "其他知情同意书"."门诊就诊流水号" IS '为门诊就诊时，填写门诊就诊流水号，住院就诊流水号填“-”';
COMMENT ON COLUMN "其他知情同意书"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "其他知情同意书"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
COMMENT ON COLUMN "其他知情同意书"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "其他知情同意书"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "其他知情同意书"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "其他知情同意书"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "其他知情同意书"."其他知情同意书流水号" IS '按照某一特定编码规则赋予其他知情同意书的唯一标识';
COMMENT ON COLUMN "其他知情同意书"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "其他知情同意书"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "其他知情同意书"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "其他知情同意书"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "其他知情同意书"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "其他知情同意书"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "其他知情同意书"."性别名称" IS '个体生理性别名称';
COMMENT ON COLUMN "其他知情同意书"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "其他知情同意书"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "其他知情同意书"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "其他知情同意书"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "其他知情同意书"."知情同意内容" IS '知情同意书中涉及的诊疗项目在实施或应用过程中可能存在的风险以及其他意外或不良后果的详细描述';
COMMENT ON COLUMN "其他知情同意书"."医疗机构意见" IS '在此诊疗过程中，医疗机构对患者应尽责任的陈述以及可能面临的风险或意外情况所采取的应对措施的详细描述';
COMMENT ON COLUMN "其他知情同意书"."患者/法定代理人意见" IS '患者／法定代理人对知情同意书中告知内容的意见描述';
COMMENT ON COLUMN "其他知情同意书"."患者签名" IS '患者签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "其他知情同意书"."法定代理人姓名" IS '法定代理人签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "其他知情同意书"."法定代理人与患者的关系代码" IS '本人与特定对象的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "其他知情同意书"."法定代理人与患者的关系名称" IS '本人与特定对象的关系类别(如户主、配偶、子女、父母等)名称';
COMMENT ON COLUMN "其他知情同意书"."患者/法定代理人签名时间" IS '患者或法定代理人签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "其他知情同意书"."医师工号" IS '医师的工号';
COMMENT ON COLUMN "其他知情同意书"."医师姓名" IS '医师签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "其他知情同意书"."医师签名时间" IS '医师进行电子签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "其他知情同意书"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "其他知情同意书"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "其他知情同意书"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "其他知情同意书"."就诊次数" IS '对于门(急)诊患者，此处表示患者门(急)诊的累计次数；对于住院患者，此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "其他知情同意书"."疾病诊断名称" IS '疾病诊断在特定编码体系中的名称';
COMMENT ON COLUMN "其他知情同意书"."疾病诊断代码" IS '疾病诊断在特定编码体系中的代码';
COMMENT ON COLUMN "其他知情同意书"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "其他知情同意书"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "其他知情同意书"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "其他知情同意书"."知情同意书名称" IS '患者在医疗机构接收诊治过程中需要被告知的知情同意书的名称';
COMMENT ON TABLE "其他知情同意书" IS '除上述之外的需要告知患者情况的信息说明';


CREATE TABLE IF NOT EXISTS "入院评估记录" (
"饮酒标志" varchar (1) DEFAULT NULL,
 "饮酒频率名称" varchar (50) DEFAULT NULL,
 "日饮酒量(mL)" decimal (3,
 0) DEFAULT NULL,
 "护理观察项目名称" varchar (200) DEFAULT NULL,
 "护理观察结果" text,
 "通知医师标志" varchar (1) DEFAULT NULL,
 "脐部情况" text,
 "通知医师时间" timestamp DEFAULT NULL,
 "评估时间" timestamp DEFAULT NULL,
 "责任护士工号" varchar (20) DEFAULT NULL,
 "责任护士姓名" varchar (50) DEFAULT NULL,
 "接诊护士工号" varchar (20) DEFAULT NULL,
 "接诊护士姓名" varchar (50) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "吸烟标志" varchar (1) DEFAULT NULL,
 "家族史" text,
 "过敏史" text,
 "输血史" text,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "入院诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "入院诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "入病房方式" varchar (20) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "联系人电话号码" varchar (20) DEFAULT NULL,
 "联系人姓名" varchar (50) DEFAULT NULL,
 "电子邮件地址" varchar (70) DEFAULT NULL,
 "工作单位电话号码" varchar (20) DEFAULT NULL,
 "患者电话号码" varchar (20) DEFAULT NULL,
 "学历代码" varchar (5) DEFAULT NULL,
 "职业类别代码" varchar (4) DEFAULT NULL,
 "婚姻状况名称" varchar (50) DEFAULT NULL,
 "婚姻状况代码" varchar (2) DEFAULT NULL,
 "民族名称" varchar (50) DEFAULT NULL,
 "民族代码" varchar (2) DEFAULT NULL,
 "国籍代码" varchar (10) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "入院评估记录流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "饮酒频率代码" varchar (2) DEFAULT NULL,
 "手术史" text,
 "预防接种史" text,
 "传染病史" text,
 "患者传染性标志" varchar (1) DEFAULT NULL,
 "疾病史(含外伤)" text,
 "一般健康状况标志" varchar (1) DEFAULT NULL,
 "生活自理能力名称" varchar (50) DEFAULT NULL,
 "自理能力代码" varchar (1) DEFAULT NULL,
 "营养状态名称" varchar (50) DEFAULT NULL,
 "营养状态代码" varchar (1) DEFAULT NULL,
 "心理状态名称" varchar (50) DEFAULT NULL,
 "心理状态代码" varchar (2) DEFAULT NULL,
 "特殊情况" text,
 "睡眠状况" text,
 "精神状态正常标志" varchar (1) DEFAULT NULL,
 "发育程度名称" varchar (30) DEFAULT NULL,
 "发育程度代码" varchar (1) DEFAULT NULL,
 "饮食情况名称" varchar (50) DEFAULT NULL,
 "饮食情况代码" varchar (2) DEFAULT NULL,
 "Apgar评分值" decimal (2,
 0) DEFAULT NULL,
 "入院途径名称" varchar (50) DEFAULT NULL,
 "入院途径代码" varchar (4) DEFAULT NULL,
 "入院原因" text,
 "主要症状" varchar (50) DEFAULT NULL,
 "脉率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "呼吸频率(次/min)" decimal (3,
 0) DEFAULT NULL,
 "停止吸烟天数" varchar (5) DEFAULT NULL,
 "吸烟状况代码" varchar (2) DEFAULT NULL,
 "吸烟状况名称" varchar (50) DEFAULT NULL,
 "日吸烟量(支)" decimal (3,
 0) DEFAULT NULL,
 CONSTRAINT "入院评估记录"_"入院评估记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("入院评估记录流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "入院评估记录"."日吸烟量(支)" IS '最近1个月内个体平均每天的吸烟量，计量单位为支';
COMMENT ON COLUMN "入院评估记录"."吸烟状况名称" IS '个体现在吸烟频率的标准名称，如现在每天吸；现在吸，但不是每天吸；过去吸，现在不吸；从不吸';
COMMENT ON COLUMN "入院评估记录"."吸烟状况代码" IS '个体过去和现在的吸烟情况(如从不吸烟；过去吸，已戒烟；吸烟)在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."停止吸烟天数" IS '本人停止吸烟的时间长度，计量单位为d';
COMMENT ON COLUMN "入院评估记录"."呼吸频率(次/min)" IS '受检者单位时间内呼吸的次数，计量单位为次/min';
COMMENT ON COLUMN "入院评估记录"."脉率(次/min)" IS '每分钟脉搏的次数测量值，计量单位为次/min';
COMMENT ON COLUMN "入院评估记录"."主要症状" IS '对个体出现主要症状的详细描述';
COMMENT ON COLUMN "入院评估记录"."入院原因" IS '此次住院的原因，如是否卫生机构转诊、体检、分娩等';
COMMENT ON COLUMN "入院评估记录"."入院途径代码" IS '患者收治入院治疗的来源分类(如门诊、急诊、其他医疗机构转入等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."入院途径名称" IS '患者收治入院治疗的来源分类(如门诊、急诊、其他医疗机构转入等)在特定编码体系中的名称码';
COMMENT ON COLUMN "入院评估记录"."Apgar评分值" IS '对新生儿娩出后呼吸、心率、皮肤颜色、肌张力及对刺激的反应等5 项指标的评分结果值，计量单位为分';
COMMENT ON COLUMN "入院评估记录"."饮食情况代码" IS '个体饮食情况所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."饮食情况名称" IS '个体饮食情况所属类别的标准名称，如良好、一般、较差等';
COMMENT ON COLUMN "入院评估记录"."发育程度代码" IS '发育情况检查结果在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."发育程度名称" IS '发育情况检查结果名称';
COMMENT ON COLUMN "入院评估记录"."精神状态正常标志" IS '标识患者精神状态是否正常';
COMMENT ON COLUMN "入院评估记录"."睡眠状况" IS '对患者睡眠情况评判结果在特定编码体系中的名称';
COMMENT ON COLUMN "入院评估记录"."特殊情况" IS '对存在特殊情况的描述';
COMMENT ON COLUMN "入院评估记录"."心理状态代码" IS '个体心理状况的分类在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."心理状态名称" IS '个体心理状况的分类在特定编码体系中的名称';
COMMENT ON COLUMN "入院评估记录"."营养状态代码" IS '受检者营养情况检查结果(如良好、中等、差等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."营养状态名称" IS '受检者营养情况检查结果';
COMMENT ON COLUMN "入院评估记录"."自理能力代码" IS '自己基本生活照料能力在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."生活自理能力名称" IS '自己基本生活照料能力在特定编码体系中的名称';
COMMENT ON COLUMN "入院评估记录"."一般健康状况标志" IS '标识患者是否一般健康状况的标志';
COMMENT ON COLUMN "入院评估记录"."疾病史(含外伤)" IS '对患者既往健康状况和疾病(含外伤)的详细描述';
COMMENT ON COLUMN "入院评估记录"."患者传染性标志" IS '标识患者是否具有传染性的标志';
COMMENT ON COLUMN "入院评估记录"."传染病史" IS '患者既往所患各种急性或慢性传染性疾病名称的详细描述';
COMMENT ON COLUMN "入院评估记录"."预防接种史" IS '患者预防接种情况的详细描述';
COMMENT ON COLUMN "入院评估记录"."手术史" IS '对患者既往接受手术/操作经历的详细描述';
COMMENT ON COLUMN "入院评估记录"."饮酒频率代码" IS '患者饮酒频率分类(如从不、偶尔、少于1d/月等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "入院评估记录"."医疗机构名称" IS '就诊医疗机构的名称';
COMMENT ON COLUMN "入院评估记录"."入院评估记录流水号" IS '按照某一特定编码规则赋予入院评估记录的顺序号';
COMMENT ON COLUMN "入院评估记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "入院评估记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "入院评估记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "入院评估记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "入院评估记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "入院评估记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "入院评估记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "入院评估记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "入院评估记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "入院评估记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院评估记录"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."性别名称" IS '个体生理性别名称';
COMMENT ON COLUMN "入院评估记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "入院评估记录"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "入院评估记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "入院评估记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "入院评估记录"."国籍代码" IS '所属国籍在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."民族名称" IS '所属民族的名称';
COMMENT ON COLUMN "入院评估记录"."婚姻状况代码" IS '当前婚姻状况(已婚、未婚、初婚等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."婚姻状况名称" IS '当前婚姻状况(已婚、未婚、初婚等)在标准特定编码体系中的名称';
COMMENT ON COLUMN "入院评估记录"."职业类别代码" IS '本人从事职业所属类别(如国家公务员、专业技术人员、职员、工人等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."学历代码" IS '个体受教育最高程度的类别(如研究生教育、大学本科、专科教育等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院评估记录"."患者电话号码" IS '患者本人的手机号码';
COMMENT ON COLUMN "入院评估记录"."工作单位电话号码" IS '所在的工作单位的电话号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "入院评估记录"."电子邮件地址" IS '患者的电子邮箱地址';
COMMENT ON COLUMN "入院评估记录"."联系人姓名" IS '联系人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院评估记录"."联系人电话号码" IS '联系人本人的手机号码';
COMMENT ON COLUMN "入院评估记录"."住院次数" IS '患者住院次数统计';
COMMENT ON COLUMN "入院评估记录"."入病房方式" IS '患患者入病房时采用的方式，如步行、轮椅等';
COMMENT ON COLUMN "入院评估记录"."入院诊断-西医诊断代码" IS '患者入院诊断西医诊断在平台编码规则体系中唯一标识';
COMMENT ON COLUMN "入院评估记录"."入院诊断-西医诊断名称" IS '患者入院诊断西医诊断在平台编码规则体系中名称';
COMMENT ON COLUMN "入院评估记录"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "入院评估记录"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "入院评估记录"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "入院评估记录"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "入院评估记录"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "入院评估记录"."输血史" IS '对患者既往输血史的详细描述';
COMMENT ON COLUMN "入院评估记录"."过敏史" IS '患者既往发生过敏情况的详细描述';
COMMENT ON COLUMN "入院评估记录"."家族史" IS '患者3代以内有血缘关系的家族成员中所患遗传疾病史的描述';
COMMENT ON COLUMN "入院评估记录"."吸烟标志" IS '标识个体是否吸烟的标志';
COMMENT ON COLUMN "入院评估记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "入院评估记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "入院评估记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "入院评估记录"."签名时间" IS '护士在护理记录上完成签名的公元纪年和日期的完整描述';
COMMENT ON COLUMN "入院评估记录"."接诊护士姓名" IS '接诊护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院评估记录"."接诊护士工号" IS '接诊护士在机构内编码体系中的编号';
COMMENT ON COLUMN "入院评估记录"."责任护士姓名" IS '责任护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院评估记录"."责任护士工号" IS '责任护士在机构内编码体系中的编号';
COMMENT ON COLUMN "入院评估记录"."评估时间" IS '评估当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "入院评估记录"."通知医师时间" IS '评估结果通知医师时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "入院评估记录"."脐部情况" IS '患者脐部情况的详细描述';
COMMENT ON COLUMN "入院评估记录"."通知医师标志" IS '标识评估结果是否通知医师';
COMMENT ON COLUMN "入院评估记录"."护理观察结果" IS '对护理观察项目结果的详细描述';
COMMENT ON COLUMN "入院评估记录"."护理观察项目名称" IS '护理观察项目的名称，如患者神志状态、饮食情况，皮肤情况、氧疗情况、排尿排便情况，流量、出量、人量等等，根据护理内容的不同选择不同的观察项目名称';
COMMENT ON COLUMN "入院评估记录"."日饮酒量(mL)" IS '个体平均每天的饮酒量，计量单位为ml';
COMMENT ON COLUMN "入院评估记录"."饮酒频率名称" IS '患者饮酒频率分类的标准名称，如从不、偶尔、少于1d/月等';
COMMENT ON COLUMN "入院评估记录"."饮酒标志" IS '标识个体是否饮酒的标志';
COMMENT ON TABLE "入院评估记录" IS '入院当时的各项评估指标结果和病史信息';


CREATE TABLE IF NOT EXISTS "入院登记信息" (
"操作员姓名" varchar (50) DEFAULT NULL,
 "新冠感染情况名称" varchar (10) DEFAULT NULL,
 "新冠感染情况代码" varchar (1) DEFAULT NULL,
 "作废标志" varchar (1) DEFAULT NULL,
 "联系人电话号码" varchar (20) DEFAULT NULL,
 "联系人姓名" varchar (50) DEFAULT NULL,
 "工作单位" varchar (128) DEFAULT NULL,
 "家庭住址" varchar (128) DEFAULT NULL,
 "监护人证件号码" varchar (32) DEFAULT NULL,
 "监护人证件类型代码" varchar (2) DEFAULT NULL,
 "监护人姓名" varchar (50) DEFAULT NULL,
 "转诊机构名称" varchar (50) DEFAULT NULL,
 "转诊机构代码" varchar (22) DEFAULT NULL,
 "转诊标志" varchar (1) DEFAULT NULL,
 "特殊患者类别代码" varchar (5) DEFAULT NULL,
 "主要病情描述" varchar (200) DEFAULT NULL,
 "入院途径名称" varchar (4) DEFAULT NULL,
 "留观标志" varchar (1) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "保险类型代码" varchar (2) DEFAULT NULL,
 "入院医保诊断名称" varchar (100) DEFAULT NULL,
 "入院医保诊断代码" varchar (20) DEFAULT NULL,
 "入院诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "入院诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "入院病区名称" varchar (100) DEFAULT NULL,
 "入院病区代码" varchar (20) DEFAULT NULL,
 "入院科室名称" varchar (100) DEFAULT NULL,
 "入院科室代码" varchar (20) DEFAULT NULL,
 "入院途径代码" varchar (4) DEFAULT NULL,
 "医保卡号" varchar (64) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "住院就诊流水号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "操作员工号" varchar (20) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 CONSTRAINT "入院登记信息"_"住院就诊流水号"_"医疗机构代码"_PK PRIMARY KEY ("住院就诊流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "入院登记信息"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "入院登记信息"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "入院登记信息"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "入院登记信息"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "入院登记信息"."操作员工号" IS '操作员在机构内编码体系中的编号';
COMMENT ON COLUMN "入院登记信息"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "入院登记信息"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "入院登记信息"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "入院登记信息"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "入院登记信息"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "入院登记信息"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院登记信息"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "入院登记信息"."证件类型代码" IS '患者身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "入院登记信息"."证件号码" IS '患者各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "入院登记信息"."姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院登记信息"."性别代码" IS '患者生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "入院登记信息"."出生日期" IS '患者出生当日的公元纪年日期';
COMMENT ON COLUMN "入院登记信息"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "入院登记信息"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "入院登记信息"."医保卡号" IS '参合人所参加的医疗保险使用卡的编号，即医保局确定一个医保参保人员的唯一编号(卡号一般为16位数)';
COMMENT ON COLUMN "入院登记信息"."入院途径代码" IS '患者收治入院治疗的来源分类(如门诊、急诊、其他医疗机构转入等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院登记信息"."入院科室代码" IS '患者入院科室在原始采集机构编码体系中的代码，住院患者填写入院科室代码';
COMMENT ON COLUMN "入院登记信息"."入院科室名称" IS '患者入院科室在原始采集机构编码体系中的名称，住院患者填写入院科室名称';
COMMENT ON COLUMN "入院登记信息"."入院病区代码" IS '患者入院时所住病区在机构内编码体系中的代码';
COMMENT ON COLUMN "入院登记信息"."入院病区名称" IS '医院系统内部定义的病区名称';
COMMENT ON COLUMN "入院登记信息"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "入院登记信息"."入院诊断-西医诊断代码" IS '入院疾病诊断在特定编码体系中的代码。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "入院登记信息"."入院诊断-西医诊断名称" IS '入院疾病诊断在特定编码体系中的名称。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "入院登记信息"."入院医保诊断代码" IS '入院疾病诊断在医保编码体系中的代码';
COMMENT ON COLUMN "入院登记信息"."入院医保诊断名称" IS '入院疾病诊断在医保编码体系中的名称';
COMMENT ON COLUMN "入院登记信息"."保险类型代码" IS '患者参加的医疗保险类别(如城镇职工基本医疗保险、城镇居民基本医疗保险等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院登记信息"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "入院登记信息"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "入院登记信息"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "入院登记信息"."留观标志" IS '标识是否对患者采取留院观察的标志';
COMMENT ON COLUMN "入院登记信息"."入院途径名称" IS '患者收治入院治疗的来源分类(如门诊、急诊、其他医疗机构转入等)在特定编码体系中的名称';
COMMENT ON COLUMN "入院登记信息"."主要病情描述" IS '患者就诊当时主要病情的详细描述';
COMMENT ON COLUMN "入院登记信息"."特殊患者类别代码" IS '特殊患者如高血压患者、糖尿病患者、血液透析者等类别在特定编码规则下的代码';
COMMENT ON COLUMN "入院登记信息"."转诊标志" IS '标识该患者是否为其他医院转入患者';
COMMENT ON COLUMN "入院登记信息"."转诊机构代码" IS '经由其他医疗机构诊治后转诊入院时，填写的转诊医疗机构经《医疗机构执业许可证》登记的，并按照特定编码体系填写的代码';
COMMENT ON COLUMN "入院登记信息"."转诊机构名称" IS '经由其他医疗机构诊治后转诊入院时，填写的转诊医疗机构经《医疗机构执业许可证》登记的名称';
COMMENT ON COLUMN "入院登记信息"."监护人姓名" IS '监护人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院登记信息"."监护人证件类型代码" IS '监护人身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "入院登记信息"."监护人证件号码" IS '监护人各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "入院登记信息"."家庭住址" IS '患者家庭住址的详细描述';
COMMENT ON COLUMN "入院登记信息"."工作单位" IS '个体工作单位的组织机构名称';
COMMENT ON COLUMN "入院登记信息"."联系人姓名" IS '联系人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院登记信息"."联系人电话号码" IS '联系人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "入院登记信息"."作废标志" IS '标识是否作废';
COMMENT ON COLUMN "入院登记信息"."新冠感染情况代码" IS '新冠感染情况(如未查/不详、阴性、阳性等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院登记信息"."新冠感染情况名称" IS '新冠感染情况(如未查/不详、阴性、阳性等)在特定编码体系中的名称';
COMMENT ON COLUMN "入院登记信息"."操作员姓名" IS '操作员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON TABLE "入院登记信息" IS '入院时登记的相关信息，包括入院科室、诊断数据';


CREATE TABLE IF NOT EXISTS "体格及功能检查明细表" (
"项目名称" varchar (64) DEFAULT NULL,
 "项目代码" varchar (32) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "体检流水号" varchar (64) DEFAULT NULL,
 "报告流水号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "体检明细流水号" varchar (36) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "检查时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "检查结果" varchar (128) DEFAULT NULL,
 "检查结果单位" varchar (20) DEFAULT NULL,
 "参考值范围" varchar (50) DEFAULT NULL,
 "结果提示" varchar (20) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "检查结果类型" varchar (1) DEFAULT NULL,
 CONSTRAINT "体格及功能检查明细表"_"体检明细流水号"_"医疗机构代码"_PK PRIMARY KEY ("体检明细流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "体格及功能检查明细表"."检查结果类型" IS '检查结果的数据类型(如数值型、阴阳型、文本型)在特定编码体系中的代码';
COMMENT ON COLUMN "体格及功能检查明细表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "体格及功能检查明细表"."结果提示" IS '依据参考值范围，提示的检查结果情况，如“↑”，“H”，“偏高”';
COMMENT ON COLUMN "体格及功能检查明细表"."参考值范围" IS '对结果正常参考值的详细描述。有些性激素的参考值和性别以及所处周期相关，无法拆解';
COMMENT ON COLUMN "体格及功能检查明细表"."检查结果单位" IS '定量结果计量单位';
COMMENT ON COLUMN "体格及功能检查明细表"."检查结果" IS '受检者体检明细项目的定量结果或定性结果，如5.6或<0.35，阴性等';
COMMENT ON COLUMN "体格及功能检查明细表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "体格及功能检查明细表"."检查时间" IS '检查完成时的公元纪念日期和时间的详细描述';
COMMENT ON COLUMN "体格及功能检查明细表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "体格及功能检查明细表"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "体格及功能检查明细表"."体检明细流水号" IS '按照某一特定编码规则赋予体检明细项目的顺序号';
COMMENT ON COLUMN "体格及功能检查明细表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "体格及功能检查明细表"."报告流水号" IS '按照某一特定编码规则赋予体格及功能检查报告的唯一标识';
COMMENT ON COLUMN "体格及功能检查明细表"."体检流水号" IS '按照某一特定编码规则赋予体检就诊记录的唯一标识';
COMMENT ON COLUMN "体格及功能检查明细表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "体格及功能检查明细表"."项目代码" IS '体检明细项目在特定编码体系中的代码。对于检查，明细项目和体检大类可能会相同';
COMMENT ON COLUMN "体格及功能检查明细表"."项目名称" IS '体检明细项目在特定编码体系中的名称，比如身高、体重、收缩压、谷丙转氨酶、血红蛋白等。对于检查，明细项目和体检大类可能会相同';
COMMENT ON TABLE "体格及功能检查明细表" IS '体检报告中明细项目的检测结果';


CREATE TABLE IF NOT EXISTS "住院相关病案报告数量日汇总" (
"医疗机构代码" varchar (22) NOT NULL,
 "业务日期" date NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "病案首页份数" decimal (9,
 0) DEFAULT NULL,
 "出院小结份数" decimal (9,
 0) DEFAULT NULL,
 "手术记录单份数" decimal (9,
 0) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 CONSTRAINT "住院相关病案报告数量日汇总"_"医疗机构代码"_"业务日期"_PK PRIMARY KEY ("医疗机构代码",
 "业务日期")
);
COMMENT ON COLUMN "住院相关病案报告数量日汇总"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "住院相关病案报告数量日汇总"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "住院相关病案报告数量日汇总"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "住院相关病案报告数量日汇总"."手术记录单份数" IS '日期内手术记录单总份数';
COMMENT ON COLUMN "住院相关病案报告数量日汇总"."出院小结份数" IS '日期内出院小结报告总份数';
COMMENT ON COLUMN "住院相关病案报告数量日汇总"."病案首页份数" IS '日期内住院病案首页报告总份数';
COMMENT ON COLUMN "住院相关病案报告数量日汇总"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "住院相关病案报告数量日汇总"."业务日期" IS '业务交易发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "住院相关病案报告数量日汇总"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON TABLE "住院相关病案报告数量日汇总" IS '医院日影像检查报告单数量';


CREATE TABLE IF NOT EXISTS "住院发药记录" (
"药品代码" varchar (50) DEFAULT NULL,
 "医嘱组号" varchar (32) DEFAULT NULL,
 "医嘱明细流水号" varchar (32) DEFAULT NULL,
 "医嘱单号" varchar (32) DEFAULT NULL,
 "发药单号" varchar (32) DEFAULT NULL,
 "年龄(月)" varchar (8) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "发药明细流水号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "收费明细流水号" varchar (32) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "发药时间" timestamp DEFAULT NULL,
 "发药人姓名" varchar (50) DEFAULT NULL,
 "发药人工号" varchar (20) DEFAULT NULL,
 "配药时间" timestamp DEFAULT NULL,
 "配药人姓名" varchar (50) DEFAULT NULL,
 "配药人工号" varchar (20) DEFAULT NULL,
 "药房名称" varchar (32) DEFAULT NULL,
 "药房代码" varchar (32) DEFAULT NULL,
 "退药人姓名" varchar (50) DEFAULT NULL,
 "退药人工号" varchar (20) DEFAULT NULL,
 "退药时间" timestamp DEFAULT NULL,
 "退药标志" varchar (1) DEFAULT NULL,
 "药品批次" varchar (32) DEFAULT NULL,
 "药品批号" varchar (32) DEFAULT NULL,
 "发药数量单位" varchar (20) DEFAULT NULL,
 "药品数量" varchar (20) DEFAULT NULL,
 "药品规格" varchar (60) DEFAULT NULL,
 "药品采购码" varchar (30) DEFAULT NULL,
 "医保代码" varchar (50) DEFAULT NULL,
 "药品名称" varchar (64) DEFAULT NULL,
 CONSTRAINT "住院发药记录"_"发药明细流水号"_"医疗机构代码"_PK PRIMARY KEY ("发药明细流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "住院发药记录"."药品名称" IS '药物通用名称';
COMMENT ON COLUMN "住院发药记录"."医保代码" IS '按照医保编码规则赋予登记药品的唯一标识';
COMMENT ON COLUMN "住院发药记录"."药品采购码" IS '参见附件《药品编码-YPID5》';
COMMENT ON COLUMN "住院发药记录"."药品规格" IS '药物规格的描述，如0.25g、5mg×28片/盒';
COMMENT ON COLUMN "住院发药记录"."药品数量" IS '处方中药品的总量';
COMMENT ON COLUMN "住院发药记录"."发药数量单位" IS '发药计量单位在机构内编码体系中的名称，如剂、盒等';
COMMENT ON COLUMN "住院发药记录"."药品批号" IS '按照某一特定编码规则赋予药物生产批号的唯一标志';
COMMENT ON COLUMN "住院发药记录"."药品批次" IS '药品的生产批号及有效期属性';
COMMENT ON COLUMN "住院发药记录"."退药标志" IS '标识药品是否退药的标志';
COMMENT ON COLUMN "住院发药记录"."退药时间" IS '退药完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "住院发药记录"."退药人工号" IS '退药人在原始特定编码体系中的编号';
COMMENT ON COLUMN "住院发药记录"."退药人姓名" IS '退药人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "住院发药记录"."药房代码" IS '医院内部药房代码';
COMMENT ON COLUMN "住院发药记录"."药房名称" IS '医院内部药房名称';
COMMENT ON COLUMN "住院发药记录"."配药人工号" IS '配药人在原始特定编码体系中的编号';
COMMENT ON COLUMN "住院发药记录"."配药人姓名" IS '配药人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "住院发药记录"."配药时间" IS '配药完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "住院发药记录"."发药人工号" IS '发药人在原始特定编码体系中的编号';
COMMENT ON COLUMN "住院发药记录"."发药人姓名" IS '发药人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "住院发药记录"."发药时间" IS '发药完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "住院发药记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "住院发药记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "住院发药记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "住院发药记录"."收费明细流水号" IS '按照某一特定编码规则赋予每条收费明细的顺序号';
COMMENT ON COLUMN "住院发药记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "住院发药记录"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "住院发药记录"."发药明细流水号" IS '按照某一特性编码规则赋予发药明细流水号的唯一标识';
COMMENT ON COLUMN "住院发药记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "住院发药记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "住院发药记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "住院发药记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "住院发药记录"."就诊科室代码" IS '按照机构内编码规则赋予就诊科室的唯一标识';
COMMENT ON COLUMN "住院发药记录"."就诊科室名称" IS '就诊科室的机构内名称';
COMMENT ON COLUMN "住院发药记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "住院发药记录"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "住院发药记录"."发药单号" IS '按照某一特定编码规则赋予发药顺序的唯一标识';
COMMENT ON COLUMN "住院发药记录"."医嘱单号" IS '按照某一特定编码规则赋予医嘱单的唯一性编号';
COMMENT ON COLUMN "住院发药记录"."医嘱明细流水号" IS '按照某一特定编码规则赋予每条医嘱明细的顺序号';
COMMENT ON COLUMN "住院发药记录"."医嘱组号" IS '同一组相同治疗目的医嘱的顺序号';
COMMENT ON COLUMN "住院发药记录"."药品代码" IS '按照机构内编码规则赋予登记药品的唯一标识';
COMMENT ON TABLE "住院发药记录" IS '住院的发药信息，包括药品基本信息、发药人、发药时间、退药情况等';


CREATE TABLE IF NOT EXISTS "传染病报告卡" (
"其他传染病代码" varchar (30) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "报卡流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "卡片编号" varchar (50) DEFAULT NULL,
 "报卡类别代码" varchar (2) DEFAULT NULL,
 "报卡类别名称" varchar (10) DEFAULT NULL,
 "报告地区行政区划代码" varchar (8) DEFAULT NULL,
 "报告地区行政区划名称" varchar (100) DEFAULT NULL,
 "健康档案标识符" varchar (32) DEFAULT NULL,
 "本人姓名" varchar (50) DEFAULT NULL,
 "家属姓名" varchar (50) DEFAULT NULL,
 "家属电话" varchar (20) DEFAULT NULL,
 "家庭关系代码" varchar (2) DEFAULT NULL,
 "家庭关系名称" varchar (60) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "实足年龄" decimal (6,
 0) DEFAULT NULL,
 "年龄单位" varchar (10) DEFAULT NULL,
 "工作单位" varchar (128) DEFAULT NULL,
 "联系电话号码" varchar (30) DEFAULT NULL,
 "病人归属代码" varchar (2) DEFAULT NULL,
 "病人归属名称" varchar (30) DEFAULT NULL,
 "现住详细地址" varchar (128) DEFAULT NULL,
 "现住址行政区划代码" varchar (12) DEFAULT NULL,
 "现住址-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "现住址-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "现住址-市(地区、州)代码" varchar (6) DEFAULT NULL,
 "现住址-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "现住址-县(市、区)代码" varchar (20) DEFAULT NULL,
 "现住址-县(市、区)名称" varchar (70) DEFAULT NULL,
 "现地址-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "现住址-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "现住址-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "现住址-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "现住址-门牌号码" varchar (70) DEFAULT NULL,
 "现住址邮编" varchar (6) DEFAULT NULL,
 "户籍地-详细地址" varchar (200) DEFAULT NULL,
 "户籍地-行政区划代码" varchar (12) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "户籍地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "户籍地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "户籍地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "户籍地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "户籍地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "户籍地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "户籍地-门牌号码" varchar (100) DEFAULT NULL,
 "户籍地址-邮政编码" varchar (6) DEFAULT NULL,
 "工作单位地址-详细地址" varchar (200) DEFAULT NULL,
 "工作单位地址-行政区划代码" varchar (9) DEFAULT NULL,
 "工作单位地址-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "工作单位地址-省(自治区、直辖市)名称" varchar (32) DEFAULT NULL,
 "工作单位地址-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "工作单位地址-市(地区、州)名称" varchar (32) DEFAULT NULL,
 "工作单位地址-县(市、区)代码" varchar (6) DEFAULT NULL,
 "工作单位地址-县(市、区)名称" varchar (32) DEFAULT NULL,
 "工作单位地址-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "工作单位地址-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "工作单位地址-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "工作单位地址-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "工作单位地址-门牌号码" varchar (100) DEFAULT NULL,
 "工作单位地址-邮政编码" varchar (6) DEFAULT NULL,
 "传染病患者职业代码" varchar (2) DEFAULT NULL,
 "传染病患者职业名称" varchar (30) DEFAULT NULL,
 "传染病患者其他职业" varchar (30) DEFAULT NULL,
 "诊断状态代码" varchar (2) DEFAULT NULL,
 "诊断状态名称" varchar (2) DEFAULT NULL,
 "传染病发病类别代码" varchar (2) DEFAULT NULL,
 "传染病发病类别名称" varchar (2) DEFAULT NULL,
 "诊断结果" text,
 "发病日期" date DEFAULT NULL,
 "诊断时间" timestamp DEFAULT NULL,
 "死亡日期" date DEFAULT NULL,
 "死亡时间" timestamp DEFAULT NULL,
 "传染病类别代码" varchar (30) DEFAULT NULL,
 "传染病类别名称" varchar (30) DEFAULT NULL,
 "法定传染病代码" varchar (30) DEFAULT NULL,
 "法定传染病名称" varchar (60) DEFAULT NULL,
 "其他传染病名称" varchar (100) DEFAULT NULL,
 "订正病名代码" varchar (30) DEFAULT NULL,
 "订正病名名称" varchar (100) DEFAULT NULL,
 "订正终审时间" timestamp DEFAULT NULL,
 "死亡终审时间" timestamp DEFAULT NULL,
 "审核状态代码" varchar (1) DEFAULT NULL,
 "县级审核时间" timestamp DEFAULT NULL,
 "市级审核时间" timestamp DEFAULT NULL,
 "省级审核时间" timestamp DEFAULT NULL,
 "删除时间" timestamp DEFAULT NULL,
 "补报填卡日期" date DEFAULT NULL,
 "收治机构" varchar (30) DEFAULT NULL,
 "收治状态" varchar (1) DEFAULT NULL,
 "删除用户" varchar (20) DEFAULT NULL,
 "删除原因" varchar (100) DEFAULT NULL,
 "订正用户" varchar (30) DEFAULT NULL,
 "订正报告时间" timestamp DEFAULT NULL,
 "港澳台或外籍地区代码" varchar (4) DEFAULT NULL,
 "其他疾病或其他病种保存的信息" varchar (30) DEFAULT NULL,
 "退卡原因" varchar (100) DEFAULT NULL,
 "报告单位" varchar (70) DEFAULT NULL,
 "报告医生工号" varchar (20) DEFAULT NULL,
 "报告医生姓名" varchar (50) DEFAULT NULL,
 "填卡日期" date DEFAULT NULL,
 "备注" varchar (1000) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "传染病报告卡"_"医疗机构代码"_"报卡流水号"_PK PRIMARY KEY ("医疗机构代码",
 "报卡流水号")
);
COMMENT ON COLUMN "传染病报告卡"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "传染病报告卡"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "传染病报告卡"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "传染病报告卡"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "传染病报告卡"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "传染病报告卡"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "传染病报告卡"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "传染病报告卡"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "传染病报告卡"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "传染病报告卡"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "传染病报告卡"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "传染病报告卡"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "传染病报告卡"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "传染病报告卡"."备注" IS '其他重要信息提示和补充说明';
COMMENT ON COLUMN "传染病报告卡"."填卡日期" IS '进行填卡的公元纪年日期的完整描述';
COMMENT ON COLUMN "传染病报告卡"."报告医生姓名" IS '报告医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "传染病报告卡"."报告医生工号" IS '报告医师在机构内编码体系中的编号';
COMMENT ON COLUMN "传染病报告卡"."报告单位" IS '传染病上报单位的组织机构名称';
COMMENT ON COLUMN "传染病报告卡"."退卡原因" IS '表示传染病报告卡填报不合格的具体原因';
COMMENT ON COLUMN "传染病报告卡"."其他疾病或其他病种保存的信息" IS '其他疾病或其他病种保存的信息说明';
COMMENT ON COLUMN "传染病报告卡"."港澳台或外籍地区代码" IS '港澳台或外籍地区的行政区划代码';
COMMENT ON COLUMN "传染病报告卡"."订正报告时间" IS '报告订正完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "传染病报告卡"."订正用户" IS '订正用户在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "传染病报告卡"."删除原因" IS '报告卡删除原因的详细描述';
COMMENT ON COLUMN "传染病报告卡"."删除用户" IS '报告卡删除人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "传染病报告卡"."收治状态" IS '传染病患者收治状态描述，肺结核患者填写';
COMMENT ON COLUMN "传染病报告卡"."收治机构" IS '传染病患者收治机构的组织机构名称，肺结核患者填写';
COMMENT ON COLUMN "传染病报告卡"."补报填卡日期" IS '进行补报填卡的公元纪年日期的完整描述';
COMMENT ON COLUMN "传染病报告卡"."删除时间" IS '报告卡删除时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "传染病报告卡"."省级审核时间" IS '省级审核完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "传染病报告卡"."市级审核时间" IS '市级审核完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "传染病报告卡"."县级审核时间" IS '县级审核完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "传染病报告卡"."审核状态代码" IS '审核状态在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."死亡终审时间" IS '对死亡诊断终审完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "传染病报告卡"."订正终审时间" IS '对订正诊断终审完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "传染病报告卡"."订正病名名称" IS '法定传染病订正后的诊断在特定编码体系中的名称，如HIV、HPV等';
COMMENT ON COLUMN "传染病报告卡"."订正病名代码" IS '法定传染病订正后的诊断在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."其他传染病名称" IS '非传染病防治法规定的传染性疾病的临床描述，如HIV、HPV等';
COMMENT ON COLUMN "传染病报告卡"."法定传染病名称" IS '传染病防治法规定的传染性疾病在特定编码体系中的名称，如HIV、HPV等';
COMMENT ON COLUMN "传染病报告卡"."法定传染病代码" IS '传染病防治法规定的传染性疾病在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."传染病类别名称" IS '传染病类别在在特定编码体系中的名称';
COMMENT ON COLUMN "传染病报告卡"."传染病类别代码" IS '传染病类别在在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."死亡时间" IS '患者死亡当时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "传染病报告卡"."死亡日期" IS '患者死亡当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "传染病报告卡"."诊断时间" IS '对患者所患疾病作出诊断时当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "传染病报告卡"."发病日期" IS '患者首次发病当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "传染病报告卡"."诊断结果" IS '对患者罹患疾病诊断结果的结论性描述';
COMMENT ON COLUMN "传染病报告卡"."传染病发病类别名称" IS '根据疾病起病时间长短的传染病分类在特定编码体系中的名称';
COMMENT ON COLUMN "传染病报告卡"."传染病发病类别代码" IS '根据疾病起病时间长短的传染病分类在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."诊断状态名称" IS '病例的诊断状态类型名称，如疑似病例、临床诊断病例、实验室确诊病例、病原携带者等';
COMMENT ON COLUMN "传染病报告卡"."诊断状态代码" IS '病例的诊断状态类型(如疑似病例、临床诊断病例、实验室确诊病例、病原携带者等)在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."传染病患者其他职业" IS '对传染病患者所从事其他职业的描述';
COMMENT ON COLUMN "传染病报告卡"."传染病患者职业代码" IS '传染病患者从事职业类别在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-邮政编码" IS '工作单位地址中所在行政区的邮政编码';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-门牌号码" IS '工作单位所在地址的门牌号码';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-村(街、路、弄等)名称" IS '工作单位所在地址的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-村(街、路、弄等)代码" IS '工作单位所在地址的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-乡(镇、街道办事处)名称" IS '工作单位所在地址的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-乡(镇、街道办事处)代码" IS '工作单位所在地址的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-县(市、区)名称" IS '工作单位所在地址的县(市、区)的名称';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-县(市、区)代码" IS '工作单位所在地址的县(区)的在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-市(地区、州)名称" IS '工作单位所在地址的市、地区或州的名称';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-市(地区、州)代码" IS '工作单位所在地址的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-省(自治区、直辖市)名称" IS '工作单位所在地址的省、自治区或直辖市名称';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-省(自治区、直辖市)代码" IS '工作单位所在地址的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-行政区划代码" IS '工作单位址所在地址区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "传染病报告卡"."工作单位地址-详细地址" IS '工作单位地址的详细描述';
COMMENT ON COLUMN "传染病报告卡"."户籍地址-邮政编码" IS '户籍地址中所在行政区的邮政编码';
COMMENT ON COLUMN "传染病报告卡"."户籍地-门牌号码" IS '户籍登记所在地址的门牌号码';
COMMENT ON COLUMN "传染病报告卡"."户籍地-村(街、路、弄等)名称" IS '户籍登记所在地址的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "传染病报告卡"."户籍地-村(街、路、弄等)代码" IS '户籍登记所在地址的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."户籍地-乡(镇、街道办事处)名称" IS '户籍登记所在地址的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "传染病报告卡"."户籍地-乡(镇、街道办事处)代码" IS '户籍登记所在地址的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."户籍地-县(市、区)名称" IS '户籍登记所在地址的县(市、区)的名称';
COMMENT ON COLUMN "传染病报告卡"."户籍地-县(市、区)代码" IS '户籍登记所在地址的县(区)的在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."户籍地-市(地区、州)名称" IS '户籍登记所在地址的市、地区或州的名称';
COMMENT ON COLUMN "传染病报告卡"."户籍地-市(地区、州)代码" IS '户籍登记所在地址的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."户籍地-省(自治区、直辖市)名称" IS '户籍登记所在地址的省、自治区或直辖市名称';
COMMENT ON COLUMN "传染病报告卡"."户籍地-省(自治区、直辖市)代码" IS '户籍登记所在地址的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."户籍地-行政区划代码" IS '户籍地址所在区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "传染病报告卡"."户籍地-详细地址" IS '户籍地址的详细描述';
COMMENT ON COLUMN "传染病报告卡"."现住址邮编" IS '现住地址中所在行政区的邮政编码';
COMMENT ON COLUMN "传染病报告卡"."现住址-门牌号码" IS '接种者现住地址中的门牌号码';
COMMENT ON COLUMN "传染病报告卡"."现住址-村(街、路、弄等)名称" IS '接种者现住地址中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "传染病报告卡"."现住址-村(街、路、弄等)代码" IS '接种者现住地址中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."现住址-乡(镇、街道办事处)名称" IS '接种者现住地址中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "传染病报告卡"."现地址-乡(镇、街道办事处)代码" IS '接种者现住地址中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."现住址-县(市、区)名称" IS '接种者现住地址中的县或区名称';
COMMENT ON COLUMN "传染病报告卡"."现住址-县(市、区)代码" IS '接种者现住地址中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."现住址-市(地区、州)名称" IS '接种者现住地址中的市、地区或州的名称';
COMMENT ON COLUMN "传染病报告卡"."现住址-市(地区、州)代码" IS '接种者现住地址中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."现住址-省(自治区、直辖市)名称" IS '接种者现住地址中的省、自治区或直辖市名称';
COMMENT ON COLUMN "传染病报告卡"."现住址-省(自治区、直辖市)代码" IS '接种者现住地址中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."现住址行政区划代码" IS '现住地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "传染病报告卡"."现住详细地址" IS '现住地址的详细描述';
COMMENT ON COLUMN "传染病报告卡"."病人归属名称" IS '传染病患者现住地址与就诊医院所在地区关系在特定编码体系中的名称';
COMMENT ON COLUMN "传染病报告卡"."病人归属代码" IS '传染病患者现住地址与就诊医院所在地区关系在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."联系电话号码" IS '患者本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "传染病报告卡"."工作单位" IS '传染病患者工作单位或学校的组织机构名称';
COMMENT ON COLUMN "传染病报告卡"."年龄单位" IS '年龄单位的详细描述，如年、月、天、时';
COMMENT ON COLUMN "传染病报告卡"."实足年龄" IS '个体从出生当日公元纪年日起到计算当日止生存的时间长度，按计量单位计算';
COMMENT ON COLUMN "传染病报告卡"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "传染病报告卡"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "传染病报告卡"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."证件号码" IS '个体居民身份证上的唯一法定标识符';
COMMENT ON COLUMN "传染病报告卡"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "传染病报告卡"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."家庭关系名称" IS '家庭关系类别(如配偶、子女、父母等)在特定编码体系中的名称';
COMMENT ON COLUMN "传染病报告卡"."家庭关系代码" IS '家庭关系类别(如配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."家属电话" IS '患者家人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "传染病报告卡"."家属姓名" IS '患者家人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "传染病报告卡"."本人姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "传染病报告卡"."健康档案标识符" IS '患者健康档案的唯一标识';
COMMENT ON COLUMN "传染病报告卡"."报告地区行政区划名称" IS '上报地区的行政区划名称';
COMMENT ON COLUMN "传染病报告卡"."报告地区行政区划代码" IS '上报地区的行政区划代码';
COMMENT ON COLUMN "传染病报告卡"."报卡类别名称" IS '报告类别在特定编码体系中的名称';
COMMENT ON COLUMN "传染病报告卡"."报卡类别代码" IS '报告类别在特定编码体系中的代码';
COMMENT ON COLUMN "传染病报告卡"."卡片编号" IS '按照某一特定编码规则赋予本人传染病报告卡的顺序号';
COMMENT ON COLUMN "传染病报告卡"."就诊次数" IS '就诊次数，即“第次就诊”指患者在本医疗机构诊治的次数。计量单位为次';
COMMENT ON COLUMN "传染病报告卡"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "传染病报告卡"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "传染病报告卡"."报卡流水号" IS '按照一定编码赋予报告卡的顺序号';
COMMENT ON COLUMN "传染病报告卡"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "传染病报告卡"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "传染病报告卡"."其他传染病代码" IS '非传染病防治法规定的传染性疾病在特定编码体系中的代码';
COMMENT ON TABLE "传染病报告卡" IS '传染病患者的个人基本信息、传染病信息等';


CREATE TABLE IF NOT EXISTS "人工智能辅助诊断记录" (
"出生日期" date DEFAULT NULL,
 "利用人工智能得出的诊断代码" varchar (50) DEFAULT NULL,
 "利用人工智能得出的诊断名称" varchar (200) DEFAULT NULL,
 "出院诊断代码" varchar (50) DEFAULT NULL,
 "出院诊断名称" varchar (200) DEFAULT NULL,
 "病理诊断代码" varchar (50) DEFAULT NULL,
 "病理诊断名称" varchar (256) DEFAULT NULL,
 "人工智能辅助诊断诊断医生" varchar (32) DEFAULT NULL,
 "人工智能辅助诊断软件名称" varchar (50) DEFAULT NULL,
 "人工智能辅助诊断软件开发者" varchar (50) DEFAULT NULL,
 "人工智能辅助诊断软件版本号" varchar (50) DEFAULT NULL,
 "人工智能辅助诊断依据" varchar (20) DEFAULT NULL,
 "诊断与出院诊断符合情况" decimal (1,
 0) DEFAULT NULL,
 "诊断与病理诊断符合情况" decimal (1,
 0) DEFAULT NULL,
 "每例人工智能辅助诊断时间" decimal (10,
 0) DEFAULT NULL,
 "人工诊断医生姓名" varchar (50) DEFAULT NULL,
 "填表人工号" varchar (20) DEFAULT NULL,
 "填表人姓名" varchar (50) DEFAULT NULL,
 "填表时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "AI辅助诊断记录编号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 CONSTRAINT "人工智能辅助诊断记录"_"AI辅助诊断记录编号"_"医疗机构代码"_PK PRIMARY KEY ("AI辅助诊断记录编号",
 "医疗机构代码")
);
COMMENT ON COLUMN "人工智能辅助诊断记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "人工智能辅助诊断记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "人工智能辅助诊断记录"."住院次数" IS '此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "人工智能辅助诊断记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "人工智能辅助诊断记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "人工智能辅助诊断记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "人工智能辅助诊断记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "人工智能辅助诊断记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "人工智能辅助诊断记录"."病床号" IS '患者入院时，所住床位对应的编号';
COMMENT ON COLUMN "人工智能辅助诊断记录"."病房号" IS '患者入院时，所住病房对应的编号';
COMMENT ON COLUMN "人工智能辅助诊断记录"."病区名称" IS '患者入院时，所住病区名称';
COMMENT ON COLUMN "人工智能辅助诊断记录"."就诊科室名称" IS '所在科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "人工智能辅助诊断记录"."就诊科室代码" IS '按照机构内编码规则赋予所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "人工智能辅助诊断记录"."医疗机构代码" IS '医疗机构在国家直报系统中的_12_位编码（如：_520000000001）';
COMMENT ON COLUMN "人工智能辅助诊断记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "人工智能辅助诊断记录"."AI辅助诊断记录编号" IS '按照某一特定编码规则赋予AI辅助诊断记录的唯一标识';
COMMENT ON COLUMN "人工智能辅助诊断记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "人工智能辅助诊断记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "人工智能辅助诊断记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "人工智能辅助诊断记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "人工智能辅助诊断记录"."填表时间" IS '完成填表时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "人工智能辅助诊断记录"."填表人姓名" IS '填表人在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "人工智能辅助诊断记录"."填表人工号" IS '填表人在原始特定编码体系中的编号';
COMMENT ON COLUMN "人工智能辅助诊断记录"."人工诊断医生姓名" IS '人工诊断医生在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "人工智能辅助诊断记录"."每例人工智能辅助诊断时间" IS '每例人工智能辅助诊断时长，计量单位为min';
COMMENT ON COLUMN "人工智能辅助诊断记录"."诊断与病理诊断符合情况" IS '诊断与病理诊断是否符合的情况标识';
COMMENT ON COLUMN "人工智能辅助诊断记录"."诊断与出院诊断符合情况" IS '诊断与出院诊断是否符合的情况标识';
COMMENT ON COLUMN "人工智能辅助诊断记录"."人工智能辅助诊断依据" IS '人工智能辅助诊断依据的分类代码';
COMMENT ON COLUMN "人工智能辅助诊断记录"."人工智能辅助诊断软件版本号" IS '人工智能辅助诊断软件版本号信息';
COMMENT ON COLUMN "人工智能辅助诊断记录"."人工智能辅助诊断软件开发者" IS '人工智能辅助诊断软件的开发者';
COMMENT ON COLUMN "人工智能辅助诊断记录"."人工智能辅助诊断软件名称" IS '人工智能辅助诊断软件的具体名称';
COMMENT ON COLUMN "人工智能辅助诊断记录"."人工智能辅助诊断诊断医生" IS '人工智能辅助诊断诊断医生在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "人工智能辅助诊断记录"."病理诊断名称" IS '病理诊断西医诊断在机构内编码体系中的名称';
COMMENT ON COLUMN "人工智能辅助诊断记录"."病理诊断代码" IS '按照机构内编码规则赋予病理诊断西医诊断疾病的唯一标识';
COMMENT ON COLUMN "人工智能辅助诊断记录"."出院诊断名称" IS '出院诊断西医诊断在机构内编码体系中的名称';
COMMENT ON COLUMN "人工智能辅助诊断记录"."出院诊断代码" IS '按照机构内编码规则赋予出院诊断西医诊断疾病的唯一标识';
COMMENT ON COLUMN "人工智能辅助诊断记录"."利用人工智能得出的诊断名称" IS '利用人工智能得出的西医诊断在机构内编码体系中的名称';
COMMENT ON COLUMN "人工智能辅助诊断记录"."利用人工智能得出的诊断代码" IS '按照机构内编码规则赋予利用人工智能得出的西医诊断疾病的唯一标识';
COMMENT ON COLUMN "人工智能辅助诊断记录"."出生日期" IS '患者出生当日的公元纪年日期';
COMMENT ON TABLE "人工智能辅助诊断记录" IS '人工智能辅助诊断记录，包括人工智能诊断名称、出院诊断、病理诊断以及诊断一致性判断信息';


CREATE TABLE IF NOT EXISTS "中医诊疗记录" (
"修改标志" varchar (1) DEFAULT NULL,
 "中医诊疗记录明细流水号" varchar (32) NOT NULL,
 "中医诊疗记录流水号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "中医治疗检查项目次数" decimal (3,
 0) DEFAULT NULL,
 "主治医师姓名" varchar (50) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "使用中医诊疗设备标志" varchar (1) DEFAULT NULL,
 "使用医疗机构中药制剂标志" varchar (1) DEFAULT NULL,
 "诊疗过程描述" text,
 "诊疗过程名称" text,
 "中医“四诊”观察结果" varchar (1000) DEFAULT NULL,
 "治则治法名称" varchar (100) DEFAULT NULL,
 "治则治法代码" varchar (100) DEFAULT NULL,
 "使用中医诊疗技术标志" varchar (1) DEFAULT NULL,
 "辨证施护标志" varchar (1) DEFAULT NULL,
 "中医类别项目代码" decimal (2,
 0) DEFAULT NULL,
 "中医类别项目名称" varchar (50) DEFAULT NULL,
 "中医证候名称" varchar (50) DEFAULT NULL,
 "门(急)诊诊断-中医证候名称" varchar (50) DEFAULT NULL,
 "门(急)诊诊断-中医证候代码" varchar (50) DEFAULT NULL,
 "门(急)诊诊断-中医诊断名称" varchar (50) DEFAULT NULL,
 "门(急)诊诊断-中医诊断代码" varchar (50) DEFAULT NULL,
 "实际治疗时间" timestamp DEFAULT NULL,
 "结束路径时间" timestamp DEFAULT NULL,
 "进入路径时间" timestamp DEFAULT NULL,
 "病程" varchar (50) DEFAULT NULL,
 "电话号码" varchar (32) DEFAULT NULL,
 "身份证件号码" varchar (18) DEFAULT NULL,
 "民族代码" decimal (2,
 0) DEFAULT NULL,
 "国籍代码" varchar (10) DEFAULT NULL,
 "年龄(月)" varchar (8) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "门(急)诊号" varchar (32) DEFAULT NULL,
 "挂号序号" varchar (32) DEFAULT NULL,
 "居民健康卡号" varchar (18) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "中医治疗检查项目名称" varchar (500) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 CONSTRAINT "中医诊疗记录"_"中医诊疗记录明细流水号"_"中医诊疗记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("中医诊疗记录明细流水号",
 "中医诊疗记录流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "中医诊疗记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "中医诊疗记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "中医诊疗记录"."中医治疗检查项目名称" IS '为患者进行中医诊疗检查项目的名称';
COMMENT ON COLUMN "中医诊疗记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "中医诊疗记录"."居民健康卡号" IS '患者持有的“中华人民共和国健康卡”的编号，或“就医卡号”等患者识别码，或暂不填写';
COMMENT ON COLUMN "中医诊疗记录"."挂号序号" IS '按照某一特性编码规则赋予挂号的顺序号';
COMMENT ON COLUMN "中医诊疗记录"."门(急)诊号" IS '按照某一特定编码规则赋予门(急)诊就诊对象的顺序号';
COMMENT ON COLUMN "中医诊疗记录"."就诊次数" IS '对于门(急)诊患者，此处表示患者门(急)诊的累计次数；对于住院患者，此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "中医诊疗记录"."姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "中医诊疗记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "中医诊疗记录"."出生日期" IS '患者出生当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "中医诊疗记录"."年龄(岁)" IS '患者年龄满1?周岁的实足年龄，为患者出生后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "中医诊疗记录"."年龄(月)" IS '条件必填，年龄不足1?周岁的实足年龄的月龄，以分数形式表示：分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1?个月的天数。此时患者年龄(岁)填写值为“0”';
COMMENT ON COLUMN "中医诊疗记录"."国籍代码" IS '患者所属国籍在特定编码体系中的代码';
COMMENT ON COLUMN "中医诊疗记录"."民族代码" IS '患者所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "中医诊疗记录"."身份证件号码" IS '患者的身份证件上的唯一法定标识符';
COMMENT ON COLUMN "中医诊疗记录"."电话号码" IS '患者本人的电话号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "中医诊疗记录"."病程" IS '病程记录内容的详细描述';
COMMENT ON COLUMN "中医诊疗记录"."进入路径时间" IS '患者进入本次治疗路径当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "中医诊疗记录"."结束路径时间" IS '患者结束本次治疗路径当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "中医诊疗记录"."实际治疗时间" IS '患者实际治疗时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "中医诊疗记录"."门(急)诊诊断-中医诊断代码" IS '按照机构内编码规则赋予门(急)诊诊断中医疾病的唯一标识';
COMMENT ON COLUMN "中医诊疗记录"."门(急)诊诊断-中医诊断名称" IS '门(急)诊诊断中医疾病在机构内编码体系中的名称';
COMMENT ON COLUMN "中医诊疗记录"."门(急)诊诊断-中医证候代码" IS '按照机构内编码规则赋予门(急)诊诊断中医证候的唯一标识';
COMMENT ON COLUMN "中医诊疗记录"."门(急)诊诊断-中医证候名称" IS '门(急)诊诊断中医证候在机构内编码体系中的名称';
COMMENT ON COLUMN "中医诊疗记录"."中医证候名称" IS '中医证候在机构内编码体系中的名称';
COMMENT ON COLUMN "中医诊疗记录"."中医类别项目名称" IS '中医诊疗类别在特定编码体系中的名称，如中医外治、中医骨伤、针刺等';
COMMENT ON COLUMN "中医诊疗记录"."中医类别项目代码" IS '中医诊疗类别在特定编码体系中的代码';
COMMENT ON COLUMN "中医诊疗记录"."辨证施护标志" IS '标识是否进行辨证施护的标志';
COMMENT ON COLUMN "中医诊疗记录"."使用中医诊疗技术标志" IS '标识是否使用了中医诊疗技术的标志';
COMMENT ON COLUMN "中医诊疗记录"."治则治法代码" IS '按照机构内编码规则赋予治则治法的唯一标识';
COMMENT ON COLUMN "中医诊疗记录"."治则治法名称" IS '中医治法在机构内编码体系中的名称';
COMMENT ON COLUMN "中医诊疗记录"."中医“四诊”观察结果" IS '中医“四诊”观察结果的详细描述，包括望、闻、问、切四诊内容';
COMMENT ON COLUMN "中医诊疗记录"."诊疗过程名称" IS '中医诊疗路径的名称描述';
COMMENT ON COLUMN "中医诊疗记录"."诊疗过程描述" IS '对患者诊疗过程或抢救情况的详细描述';
COMMENT ON COLUMN "中医诊疗记录"."使用医疗机构中药制剂标志" IS '标识是否使用了医疗机构中药制剂的标志';
COMMENT ON COLUMN "中医诊疗记录"."使用中医诊疗设备标志" IS '标识是否使用了中医诊疗设备的标志';
COMMENT ON COLUMN "中医诊疗记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "中医诊疗记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "中医诊疗记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "中医诊疗记录"."主治医师姓名" IS '所在科室的具有主治医师专业技术职务资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "中医诊疗记录"."中医治疗检查项目次数" IS '实施中医治疗检查项目的次数';
COMMENT ON COLUMN "中医诊疗记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "中医诊疗记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "中医诊疗记录"."中医诊疗记录流水号" IS '按照某一特性编码规则赋予中医诊疗记录的唯一标识';
COMMENT ON COLUMN "中医诊疗记录"."中医诊疗记录明细流水号" IS '按照某一特性编码规则赋予中医诊疗记录明细的唯一标识';
COMMENT ON COLUMN "中医诊疗记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON TABLE "中医诊疗记录" IS '中医诊疗记录，包括中医诊断、治疗方法，检查项目等';


CREATE TABLE IF NOT EXISTS "业务收入统计表" (
"数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "科室代码" varchar (20) NOT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "业务日期" date NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "实有床位数" decimal (15,
 3) DEFAULT NULL,
 "门急诊医疗费用" decimal (15,
 3) DEFAULT NULL,
 "住院医疗费用" decimal (15,
 3) DEFAULT NULL,
 "门急诊药品费用" decimal (15,
 3) DEFAULT NULL,
 "住院药品费用" decimal (15,
 3) DEFAULT NULL,
 "门急诊医保医疗费用" decimal (15,
 3) DEFAULT NULL,
 "住院医保医疗费用" decimal (15,
 3) DEFAULT NULL,
 "门急诊医保药品费用" decimal (15,
 3) DEFAULT NULL,
 "住院医保药品费用" decimal (15,
 3) DEFAULT NULL,
 "住院实际发生费用" decimal (15,
 3) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 CONSTRAINT "业务收入统计表"_"医疗机构代码"_"科室代码"_"业务日期"_PK PRIMARY KEY ("医疗机构代码",
 "科室代码",
 "业务日期")
);
COMMENT ON COLUMN "业务收入统计表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "业务收入统计表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "业务收入统计表"."住院实际发生费用" IS '实际发生结算时的住院医疗费用收入';
COMMENT ON COLUMN "业务收入统计表"."住院医保药品费用" IS '实际发生结算时的住院医保药品费用收入，指以“普通医保”身份就诊的患者的全部药品费用(无论是否为医保药品)';
COMMENT ON COLUMN "业务收入统计表"."门急诊医保药品费用" IS '实际发生结算时的门急诊医保药品费用收入，指以“普通医保”身份就诊的患者的全部药品费用(无论是否为医保药品)';
COMMENT ON COLUMN "业务收入统计表"."住院医保医疗费用" IS '实际发生结算时的住院医保医疗费用收入，是指以“普通医保”身份就诊的患者的全部就诊费用中扣除药品费用以外的部分';
COMMENT ON COLUMN "业务收入统计表"."门急诊医保医疗费用" IS '实际发生结算时的门急诊医保医疗费用收入，是指以“普通医保”身份就诊的患者的全部就诊费用中扣除药品费用以外的部分';
COMMENT ON COLUMN "业务收入统计表"."住院药品费用" IS '实际发生结算时的住院药品费用收入';
COMMENT ON COLUMN "业务收入统计表"."门急诊药品费用" IS '实际发生结算时的门急诊药品费用收入';
COMMENT ON COLUMN "业务收入统计表"."住院医疗费用" IS '实际发生结算时的住院医疗费用收入，不包括药品费用';
COMMENT ON COLUMN "业务收入统计表"."门急诊医疗费用" IS '实际发生结算时的门急诊医疗费用收入，不包括药品费用';
COMMENT ON COLUMN "业务收入统计表"."实有床位数" IS '实际发生结算时的实际床位数量，指年底(期末)固定实有床位数，包括正规床、简易床、监护床、超过半年的加床、正在消毒和修理床位、因扩建或大修而停用的床位。不包括产科新生儿床、接产室待产床、库存床、观察床、临时加床和病人家属陪侍床。2007版卫统上对此有专门解释。俗称“开放床位”';
COMMENT ON COLUMN "业务收入统计表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "业务收入统计表"."业务日期" IS '业务交易发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "业务收入统计表"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "业务收入统计表"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "业务收入统计表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "业务收入统计表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "业务收入统计表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON TABLE "业务收入统计表" IS '按科室统计医院日业务收入，包括门急诊、住院、药品收入等';


CREATE TABLE IF NOT EXISTS "不良事件_医疗安全事件报告" (
"数据更新时间" timestamp DEFAULT NULL,
 "上传机构代码" varchar (50) NOT NULL,
 "上传机构名称" varchar (70) DEFAULT NULL,
 "医疗安全事件报告编号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "报告日期" date DEFAULT NULL,
 "不良反应事件发生时间" timestamp DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "不良事件患者年龄" varchar (3) DEFAULT NULL,
 "职业类别代码" varchar (4) DEFAULT NULL,
 "职业类别名称" varchar (60) DEFAULT NULL,
 "就诊时间" timestamp DEFAULT NULL,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "疾病诊断名称" varchar (512) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "事件发生场所代码" varchar (2) DEFAULT NULL,
 "事件发生场所名称" varchar (20) DEFAULT NULL,
 "有不良后果标志" varchar (1) DEFAULT NULL,
 "不良事件类别代码" varchar (3) DEFAULT NULL,
 "不良事件类别名称" varchar (20) DEFAULT NULL,
 "不良事件的等级代码" varchar (10) DEFAULT NULL,
 "不良事件报告人职业代码" varchar (2) DEFAULT NULL,
 "报告人职务/职称代码" varchar (100) DEFAULT NULL,
 "不良事件当事人类别代码" varchar (2) DEFAULT NULL,
 "不良事件报告人姓名" varchar (50) DEFAULT NULL,
 "不良事件报告人联系电话" varchar (20) DEFAULT NULL,
 "不良事件报告人邮箱" varchar (50) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "文本内容" text,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 CONSTRAINT "不良事件_医疗安全事件报告"_"上传机构代码"_"医疗安全事件报告编号"_PK PRIMARY KEY ("上传机构代码",
 "医疗安全事件报告编号")
);
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."文本内容" IS '文本详细内容';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."不良事件报告人邮箱" IS '报告人的电子邮箱名称';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."不良事件报告人联系电话" IS '报告人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."不良事件报告人姓名" IS '报告人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."不良事件当事人类别代码" IS '不良事件当事人类别(如本院、进修生、研究生等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."报告人职务/职称代码" IS '报告人的职务/ 职称在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."不良事件报告人职业代码" IS '报告人的职业在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."不良事件的等级代码" IS '对本次不良事件所评定的等级在标准体系中的代码';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."不良事件类别名称" IS '不良事件类别(如信息传递错误事件、治疗错误事件等)在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."不良事件类别代码" IS '不良事件类别(如信息传递错误事件、治疗错误事件等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."有不良后果标志" IS '标识不良事件是否导致不良后果的标志';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."事件发生场所名称" IS '机构中发生医疗安全不良事件的部门(如急诊、门诊、住院部等)在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."事件发生场所代码" IS '机构中发生医疗安全不良事件的部门(如急诊、门诊、住院部等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."科室名称" IS '就诊科室在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."科室代码" IS '按照特定编码规则赋予就诊科室的唯一标识';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."疾病诊断名称" IS '由医师根据患者就诊时的情况，综合分析所作出的报告中所治疗的西医疾病西医诊断机构内名称';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."疾病诊断代码" IS '按照机构内编码规则赋予报告中所治疗的西医疾病西医疾病的唯一标识';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."就诊时间" IS '就诊当日的公元纪年和日期的完整描述';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."职业类别名称" IS '本人从事职业所属类别的标准名称，如国家公务员、专业技术人员、职员、工人等';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."职业类别代码" IS '本人从事职业所属类别(如国家公务员、专业技术人员、职员、工人等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."不良事件患者年龄" IS '个体从出生当日公元纪年日起到计算当日止生存的时间长度，按计量单位计算';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."不良反应事件发生时间" IS '不良事件发生当时的公元纪年和日期的完整描述';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."报告日期" IS '报告不良事件当时的公元纪年日期的描述';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."医疗安全事件报告编号" IS '按照某一特定编码规则赋予医疗安全不良事件报告的顺序号，是医疗安全不良事件报告的唯一标识';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."上传机构名称" IS '上传机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."上传机构代码" IS '上传机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "不良事件_医疗安全事件报告"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON TABLE "不良事件_医疗安全事件报告" IS '临床诊疗活动中发生的不良事件记录，包括相关部门、等级等信息';


CREATE TABLE IF NOT EXISTS "上级医师查房记录" (
"上级医师查房记录流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "文本内容" text,
 "签名时间" timestamp DEFAULT NULL,
 "主任医师姓名" varchar (50) DEFAULT NULL,
 "主任医师工号" varchar (20) DEFAULT NULL,
 "主治医师姓名" varchar (50) DEFAULT NULL,
 "主治医师工号" varchar (20) DEFAULT NULL,
 "记录人姓名" varchar (50) DEFAULT NULL,
 "记录人工号" varchar (20) DEFAULT NULL,
 "诊疗计划" text,
 "中药用药方法" varchar (100) DEFAULT NULL,
 "中药煎煮方法" varchar (100) DEFAULT NULL,
 "辨证论治详细描述" text,
 "中医“四诊”观察结果" text,
 "医嘱内容" text,
 "査房记录" text,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "査房时间" timestamp DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 CONSTRAINT "上级医师查房记录"_"上级医师查房记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("上级医师查房记录流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "上级医师查房记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "上级医师查房记录"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "上级医师查房记录"."住院次数" IS '办理完整住院治疗手续的次数';
COMMENT ON COLUMN "上级医师查房记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "上级医师查房记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "上级医师查房记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "上级医师查房记录"."科室代码" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "上级医师查房记录"."科室名称" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "上级医师查房记录"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "上级医师查房记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "上级医师查房记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "上级医师查房记录"."査房时间" IS '开始查房时的公元几年日期和时间的完整描述';
COMMENT ON COLUMN "上级医师查房记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "上级医师查房记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "上级医师查房记录"."性别名称" IS '一般指患者医学生理性别，指男性或女性';
COMMENT ON COLUMN "上级医师查房记录"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "上级医师查房记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "上级医师查房记录"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "上级医师查房记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "上级医师查房记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "上级医师查房记录"."査房记录" IS '对患者查房结果和诊疗相关意见的详细描述';
COMMENT ON COLUMN "上级医师查房记录"."医嘱内容" IS '医嘱内容的详细描述';
COMMENT ON COLUMN "上级医师查房记录"."中医“四诊”观察结果" IS '中医“四诊”观察结果的详细描述，包括望、闻、问、切四诊内容';
COMMENT ON COLUMN "上级医师查房记录"."辨证论治详细描述" IS '对辨证分型的名称、主要依据和采用的治则治法的详细描述';
COMMENT ON COLUMN "上级医师查房记录"."中药煎煮方法" IS '中药饮片煎煮方法描述，如水煎等';
COMMENT ON COLUMN "上级医师查房记录"."中药用药方法" IS '中药的用药方法的描述，如bid 煎服，先煎、后下等';
COMMENT ON COLUMN "上级医师查房记录"."诊疗计划" IS '具体的检査、中西医治疗措施及中医调护';
COMMENT ON COLUMN "上级医师查房记录"."记录人工号" IS '记录人员在机构特定编码体系中的编号';
COMMENT ON COLUMN "上级医师查房记录"."记录人姓名" IS '记录人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "上级医师查房记录"."主治医师工号" IS '所在科室的具有主治医师的工号';
COMMENT ON COLUMN "上级医师查房记录"."主治医师姓名" IS '所在科室的具有主治医师专业技术职务资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "上级医师查房记录"."主任医师工号" IS '主任医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "上级医师查房记录"."主任医师姓名" IS '主任医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "上级医师查房记录"."签名时间" IS '医师完成签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "上级医师查房记录"."文本内容" IS '存入大文本的内容，最大不超过64K';
COMMENT ON COLUMN "上级医师查房记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "上级医师查房记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "上级医师查房记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "上级医师查房记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "上级医师查房记录"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名';
COMMENT ON COLUMN "上级医师查房记录"."上级医师查房记录流水号" IS '按照一定编码规则赋予上级医师查房记录的唯一标识';
COMMENT ON TABLE "上级医师查房记录" IS '上级医师查房时的中医诊治、用药方法和医嘱相关内容的记录';


CREATE TABLE IF NOT EXISTS "(
高血压)患者管理评估记录" ("评估医生姓名" varchar (50) DEFAULT NULL,
 "评估医生工号" varchar (20) DEFAULT NULL,
 "评估机构名称" varchar (70) DEFAULT NULL,
 "评估机构代码" varchar (22) DEFAULT NULL,
 "评估日期" date DEFAULT NULL,
 "管理卡标识号" varchar (64) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "评估流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "下次评估日期" date DEFAULT NULL,
 "指导建议" varchar (500) DEFAULT NULL,
 "危险分层变化情况名称" varchar (50) DEFAULT NULL,
 "危险分层变化情况代码" varchar (2) DEFAULT NULL,
 "心理状况改名名称" varchar (50) DEFAULT NULL,
 "心理状况改变代码" varchar (2) DEFAULT NULL,
 "运动改名名称" varchar (50) DEFAULT NULL,
 "运动改变代码" varchar (2) DEFAULT NULL,
 "摄盐改名名称" varchar (50) DEFAULT NULL,
 "摄盐改变代码" varchar (2) DEFAULT NULL,
 "饮食改变代码" varchar (2) DEFAULT NULL,
 "饮酒改名名称" varchar (50) DEFAULT NULL,
 "饮酒改变代码" varchar (2) DEFAULT NULL,
 "吸烟改名名称" varchar (50) DEFAULT NULL,
 "吸烟改变代码" varchar (2) DEFAULT NULL,
 "辅助检查结果" varchar (1000) DEFAULT NULL,
 "辅助检查标志" varchar (1) DEFAULT NULL,
 "血压控制情况名称" varchar (50) DEFAULT NULL,
 "血压控制情况代码" varchar (2) DEFAULT NULL,
 "危险分层名称" varchar (50) DEFAULT NULL,
 "危险分层代码" varchar (2) DEFAULT NULL,
 "并存临床情况" varchar (255) DEFAULT NULL,
 "靶器官损害情况" varchar (255) DEFAULT NULL,
 "危险因素描述" varchar (255) DEFAULT NULL,
 "血压分级名称" varchar (50) DEFAULT NULL,
 "血压分级代码" varchar (2) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 CONSTRAINT "(高血压)患者管理评估记录"_"评估流水号"_"医疗机构代码"_PK PRIMARY KEY ("评估流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(高血压)患者管理评估记录"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."血压分级代码" IS '血压分级在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."血压分级名称" IS '血压分级在特定编码体系中的名称，如正常血压、正常高值等';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."危险因素描述" IS '与高血压相关的危险因素的详细描述';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."靶器官损害情况" IS '靶器官损害情况描述';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."并存临床情况" IS '并存的临床情况描述';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."危险分层代码" IS '危险程度分层在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."危险分层名称" IS '危险程度分层在特定编码体系中的名称，如低危层、中危层等';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."血压控制情况代码" IS '血压控制情况在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."血压控制情况名称" IS '血压控制情况在特定编码体系中的名称，如优良、不良层等';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."辅助检查标志" IS '标识患者是否做辅助检查';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."辅助检查结果" IS '受检者辅助检查结果的详细描述';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."吸烟改变代码" IS '吸烟改变程度(如未改变、部分改变、完全改变)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."吸烟改名名称" IS '吸烟改变程度名称，如未改变、部分改变、完全改变';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."饮酒改变代码" IS '饮酒改变程度(如未改变、部分改变、完全改变)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."饮酒改名名称" IS '饮酒改变程度名称，如未改变、部分改变、完全改变';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."饮食改变代码" IS '饮食改变程度(如未改变、部分改变、完全改变)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."摄盐改变代码" IS '摄盐改变程度(如未改变、部分改变、完全改变)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."摄盐改名名称" IS '摄盐改变程度名称，如未改变、部分改变、完全改变';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."运动改变代码" IS '运动改变程度(如未改变、部分改变、完全改变)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."运动改名名称" IS '运动改变程度名称，如未改变、部分改变、完全改变';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."心理状况改变代码" IS '心理状况改变程度(如未改变、部分改变、完全改变)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."心理状况改名名称" IS '心理状况改变程度名称，如未改变、部分改变、完全改变';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."危险分层变化情况代码" IS '危险分层变化方向在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."危险分层变化情况名称" IS '危险分层变化方向在特定编码体系中的名称，如初次评估、不变、上升等';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."指导建议" IS '描述医师针对病人情况而提出的指导建议';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."下次评估日期" IS '对患者下次进行评估当日的公元纪年日期';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."评估流水号" IS '按照一定编码规则赋予产后访视记录的顺序号';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."管理卡标识号" IS '按照某一特定编码规则赋予管理卡的唯一标识号';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."评估日期" IS '对患者进行评估当日的公元纪年日期';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."评估机构代码" IS '评估医疗机构的组织机构代码';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."评估机构名称" IS '评估机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."评估医生工号" IS '评估医师的工号';
COMMENT ON COLUMN "(高血压)患者管理评估记录"."评估医生姓名" IS '评估医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON TABLE "(高血压)患者管理评估记录" IS '高血压患者评估信息，包括收缩压和舒张压检查值、血压分级、危险因素、靶器官损害等';


CREATE TABLE IF NOT EXISTS "(
高血压)患者用药记录" ("药物使用途径代码" varchar (32) DEFAULT NULL,
 "药物使用总剂量" decimal (11,
 2) DEFAULT NULL,
 "药物使用次剂量" decimal (4,
 2) DEFAULT NULL,
 "药物使用剂量单位" varchar (6) DEFAULT NULL,
 "药物使用频率名称" varchar (20) DEFAULT NULL,
 "药物名称" varchar (50) DEFAULT NULL,
 "中药类别名称" varchar (50) DEFAULT NULL,
 "中药类别代码" varchar (2) DEFAULT NULL,
 "随访流水号" varchar (64) DEFAULT NULL,
 "管理卡标识号" varchar (64) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "用药流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "用药停止时间" timestamp DEFAULT NULL,
 "用药开始时间" timestamp DEFAULT NULL,
 "药物使用途径名称" varchar (50) DEFAULT NULL,
 CONSTRAINT "(高血压)患者用药记录"_"用药流水号"_"医疗机构代码"_PK PRIMARY KEY ("用药流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(高血压)患者用药记录"."药物使用途径名称" IS '药物的给药途径在特定编码体系中的名称，如口服、静滴、喷喉等';
COMMENT ON COLUMN "(高血压)患者用药记录"."用药开始时间" IS '用药开始时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者用药记录"."用药停止时间" IS '用药结束时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者用药记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者用药记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(高血压)患者用药记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(高血压)患者用药记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(高血压)患者用药记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(高血压)患者用药记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者用药记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(高血压)患者用药记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(高血压)患者用药记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(高血压)患者用药记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(高血压)患者用药记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(高血压)患者用药记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者用药记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者用药记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(高血压)患者用药记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(高血压)患者用药记录"."用药流水号" IS '按照一定编码规则赋予产后访视记录的顺序号';
COMMENT ON COLUMN "(高血压)患者用药记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(高血压)患者用药记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(高血压)患者用药记录"."管理卡标识号" IS '按照某一特定编码规则赋予管理卡的唯一标识号';
COMMENT ON COLUMN "(高血压)患者用药记录"."随访流水号" IS '按照一定编码规则赋予评估记录的顺序号';
COMMENT ON COLUMN "(高血压)患者用药记录"."中药类别代码" IS '中药使用类别(如未使用、中成药、中草药、其他中药)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者用药记录"."中药类别名称" IS '中药使用类别的标准名称，如未使用、中成药、中草药、其他中药等';
COMMENT ON COLUMN "(高血压)患者用药记录"."药物名称" IS '药品在特定编码体系中的名称。对于医疗机构制剂，此处填写医疗机构制剂名称';
COMMENT ON COLUMN "(高血压)患者用药记录"."药物使用频率名称" IS '单位时间内药物使用次数在机构内编码体系中的名称，如每天两次、每周两次、睡前一次等';
COMMENT ON COLUMN "(高血压)患者用药记录"."药物使用剂量单位" IS '药物使用剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "(高血压)患者用药记录"."药物使用次剂量" IS '单次使用药物的剂量，按剂量单位计';
COMMENT ON COLUMN "(高血压)患者用药记录"."药物使用总剂量" IS '在一定时间段内使用药物的总量。根据单次剂量、频次计算得到的总量';
COMMENT ON COLUMN "(高血压)患者用药记录"."药物使用途径代码" IS '药物使用途径(如口服、静滴、喷喉等)在特定编码体系中的代码';
COMMENT ON TABLE "(高血压)患者用药记录" IS '高血压患者用药记录，包括药物类别、名称、使用频率、剂量、途径等';


CREATE TABLE IF NOT EXISTS "(
脑卒中)患者用药记录" ("药物使用总剂量" decimal (11,
 2) DEFAULT NULL,
 "药物使用次剂量" decimal (4,
 2) DEFAULT NULL,
 "药物使用剂量单位" varchar (6) DEFAULT NULL,
 "药物使用频率名称" varchar (20) DEFAULT NULL,
 "药物名称" varchar (50) DEFAULT NULL,
 "中药类别名称" varchar (50) DEFAULT NULL,
 "中药类别代码" varchar (2) DEFAULT NULL,
 "随访流水号" varchar (64) DEFAULT NULL,
 "专项档案标识号" varchar (64) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "用药流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "药物使用途径名称" varchar (50) DEFAULT NULL,
 "药物使用途径代码" varchar (32) DEFAULT NULL,
 CONSTRAINT "(脑卒中)患者用药记录"_"用药流水号"_"医疗机构代码"_PK PRIMARY KEY ("用药流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(脑卒中)患者用药记录"."药物使用途径代码" IS '药物使用途径(如口服、静滴、喷喉等)在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."药物使用途径名称" IS '药物的给药途径在特定编码体系中的名称，如口服、静滴、喷喉等';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."用药流水号" IS '按照一定编码规则赋予用药记录的顺序号';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."专项档案标识号" IS '按照某一特定规则赋予专项档案的唯一标识';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."随访流水号" IS '按照一定编码规则赋予随访记录的顺序号';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."中药类别代码" IS '中药使用类别(如未使用、中成药、中草药、其他中药)在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."中药类别名称" IS '中药使用类别的标准名称，如未使用、中成药、中草药、其他中药等';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."药物名称" IS '药品在特定编码体系中的名称。对于医疗机构制剂，此处填写医疗机构制剂名称';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."药物使用频率名称" IS '单位时间内药物使用频次类别的标准名称，如每天两次、每周两次、睡前一次等';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."药物使用剂量单位" IS '药物使用剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."药物使用次剂量" IS '单次使用药物的剂量，按剂量单位计';
COMMENT ON COLUMN "(脑卒中)患者用药记录"."药物使用总剂量" IS '在一定时间段内使用药物的总量。根据单次剂量、频次计算得到的总量';
COMMENT ON TABLE "(脑卒中)患者用药记录" IS '脑卒中患者用药记录，包括药物类别、名称、使用频率、剂量、途径等';


CREATE TABLE IF NOT EXISTS "(
肿瘤)患者家族史" ("患者与本人关系名称" varchar (50) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "肿瘤家族史瘤别代码" varchar (60) DEFAULT NULL,
 "肿瘤家族史瘤别名称" varchar (25) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "报告卡标识号" varchar (64) NOT NULL,
 "患者与本人关系代码" varchar (2) NOT NULL,
 CONSTRAINT "(肿瘤)患者家族史"_"医疗机构代码"_"报告卡标识号"_"患者与本人关系代码"_PK PRIMARY KEY ("医疗机构代码",
 "报告卡标识号",
 "患者与本人关系代码")
);
COMMENT ON COLUMN "(肿瘤)患者家族史"."患者与本人关系代码" IS '家族肿瘤患者与患者关系的家庭关系在特定编码体系中的代码';
COMMENT ON COLUMN "(肿瘤)患者家族史"."报告卡标识号" IS '按照某一特定规则赋予报告卡的唯一标识';
COMMENT ON COLUMN "(肿瘤)患者家族史"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(肿瘤)患者家族史"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(肿瘤)患者家族史"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(肿瘤)患者家族史"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(肿瘤)患者家族史"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(肿瘤)患者家族史"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(肿瘤)患者家族史"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(肿瘤)患者家族史"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(肿瘤)患者家族史"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(肿瘤)患者家族史"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(肿瘤)患者家族史"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(肿瘤)患者家族史"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(肿瘤)患者家族史"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(肿瘤)患者家族史"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(肿瘤)患者家族史"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(肿瘤)患者家族史"."肿瘤家族史瘤别名称" IS '肿瘤家族史瘤别诊断在特定编码体系中的名称。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "(肿瘤)患者家族史"."肿瘤家族史瘤别代码" IS '肿瘤家族史瘤别诊断在特定编码体系中的代码。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "(肿瘤)患者家族史"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(肿瘤)患者家族史"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(肿瘤)患者家族史"."患者与本人关系名称" IS '家族肿瘤患者与患者关系的标准名称,如配偶、子女、父母等';
COMMENT ON TABLE "(肿瘤)患者家族史" IS '肿瘤患者家族史，包括家族史瘤别、与患者关系等';


CREATE TABLE IF NOT EXISTS "(
老年人)用药记录" ("药物使用途径名称" varchar (50) DEFAULT NULL,
 "药物使用途径代码" varchar (32) DEFAULT NULL,
 "药物使用总剂量" decimal (11,
 2) DEFAULT NULL,
 "药物使用次剂量" decimal (4,
 2) DEFAULT NULL,
 "药物使用剂量单位" varchar (6) DEFAULT NULL,
 "药物使用频率名称" varchar (20) DEFAULT NULL,
 "药物名称" varchar (50) DEFAULT NULL,
 "中药类别名称" varchar (50) DEFAULT NULL,
 "中药类别代码" varchar (2) DEFAULT NULL,
 "随访流水号" varchar (64) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "用药流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 CONSTRAINT "(老年人)用药记录"_"用药流水号"_"医疗机构代码"_PK PRIMARY KEY ("用药流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(老年人)用药记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(老年人)用药记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(老年人)用药记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(老年人)用药记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(老年人)用药记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(老年人)用药记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(老年人)用药记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(老年人)用药记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(老年人)用药记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(老年人)用药记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(老年人)用药记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(老年人)用药记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(老年人)用药记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(老年人)用药记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(老年人)用药记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(老年人)用药记录"."用药流水号" IS '按照一定编码规则赋予产后访视记录的顺序号';
COMMENT ON COLUMN "(老年人)用药记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(老年人)用药记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(老年人)用药记录"."随访流水号" IS '按照一定编码规则赋予评估记录的顺序号';
COMMENT ON COLUMN "(老年人)用药记录"."中药类别代码" IS '中药使用类别(如未使用、中成药、中草药、其他中药)在特定编码体系中的代码';
COMMENT ON COLUMN "(老年人)用药记录"."中药类别名称" IS '中药使用类别的标准名称，如未使用、中成药、中草药、其他中药等';
COMMENT ON COLUMN "(老年人)用药记录"."药物名称" IS '药品在特定编码体系中的名称。对于医疗机构制剂，此处填写医疗机构制剂名称';
COMMENT ON COLUMN "(老年人)用药记录"."药物使用频率名称" IS '单位时间内药物使用频次类别的标准名称，如每天两次、每周两次、睡前一次等';
COMMENT ON COLUMN "(老年人)用药记录"."药物使用剂量单位" IS '药物使用剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "(老年人)用药记录"."药物使用次剂量" IS '单次使用药物的剂量，按剂量单位计';
COMMENT ON COLUMN "(老年人)用药记录"."药物使用总剂量" IS '在一定时间段内使用药物的总量。根据单次剂量、频次计算得到的总量';
COMMENT ON COLUMN "(老年人)用药记录"."药物使用途径代码" IS '药物使用途径(如口服、静滴、喷喉等)在特定编码体系中的代码';
COMMENT ON COLUMN "(老年人)用药记录"."药物使用途径名称" IS '药物的给药途径在特定编码体系中的名称，如口服、静滴、喷喉等';
COMMENT ON TABLE "(老年人)用药记录" IS '老年人用药记录，包括药物类别、名称、使用频率、剂量、途径等';


CREATE TABLE IF NOT EXISTS "(
群体)活动记录" ("活动总结评价" varchar (1000) DEFAULT NULL,
 "活动内容" text,
 "资料发放种类及数量" varchar (100) DEFAULT NULL,
 "接受教育人数" decimal (6,
 0) DEFAULT NULL,
 "接受教育人员类别" varchar (100) DEFAULT NULL,
 "组织人员" varchar (100) DEFAULT NULL,
 "活动主题" varchar (100) DEFAULT NULL,
 "活动形式" varchar (100) DEFAULT NULL,
 "活动地点" varchar (70) DEFAULT NULL,
 "活动日期" date DEFAULT NULL,
 "负责人员姓名" varchar (50) DEFAULT NULL,
 "负责人员工号" varchar (20) DEFAULT NULL,
 "填表人员姓名" varchar (50) DEFAULT NULL,
 "填表人员工号" varchar (20) DEFAULT NULL,
 "活动机构名称" varchar (70) DEFAULT NULL,
 "活动机构代码" varchar (22) DEFAULT NULL,
 "填表日期" date DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "活动流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "存档材料其他" varchar (100) DEFAULT NULL,
 "存档材料名称" varchar (100) DEFAULT NULL,
 "存档材料代码" varchar (30) DEFAULT NULL,
 CONSTRAINT "(群体)活动记录"_"活动流水号"_"医疗机构代码"_PK PRIMARY KEY ("活动流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(群体)活动记录"."存档材料代码" IS '存档材料(如书面材料、图片材料、印刷材料、影音材料、签到表等)在特定编码体系中的代码';
COMMENT ON COLUMN "(群体)活动记录"."存档材料名称" IS '存档材料(如书面材料、图片材料、印刷材料、影音材料、签到表等)在特定编码体系中的名称';
COMMENT ON COLUMN "(群体)活动记录"."存档材料其他" IS '除上述存档材料类别外，其他存档材料的名称';
COMMENT ON COLUMN "(群体)活动记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(群体)活动记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(群体)活动记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(群体)活动记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(群体)活动记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(群体)活动记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(群体)活动记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(群体)活动记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(群体)活动记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(群体)活动记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(群体)活动记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(群体)活动记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(群体)活动记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(群体)活动记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(群体)活动记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(群体)活动记录"."活动流水号" IS '按照某一特定编码规则赋予随访记录的顺序号';
COMMENT ON COLUMN "(群体)活动记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(群体)活动记录"."填表日期" IS '完成填表时的公元纪年日期';
COMMENT ON COLUMN "(群体)活动记录"."活动机构代码" IS '按照某一特定编码规则赋予活动机构的唯一标识';
COMMENT ON COLUMN "(群体)活动记录"."活动机构名称" IS '活动机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(群体)活动记录"."填表人员工号" IS '填表人员的工号';
COMMENT ON COLUMN "(群体)活动记录"."填表人员姓名" IS '填表人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(群体)活动记录"."负责人员工号" IS '负责人的工号';
COMMENT ON COLUMN "(群体)活动记录"."负责人员姓名" IS '负责人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(群体)活动记录"."活动日期" IS '开展活动时的公元纪年日期';
COMMENT ON COLUMN "(群体)活动记录"."活动地点" IS '活动地点的详细描述';
COMMENT ON COLUMN "(群体)活动记录"."活动形式" IS '宣教活动形式的描述';
COMMENT ON COLUMN "(群体)活动记录"."活动主题" IS '宣教的活动主题描述';
COMMENT ON COLUMN "(群体)活动记录"."组织人员" IS '组织活动的人员姓名列表';
COMMENT ON COLUMN "(群体)活动记录"."接受教育人员类别" IS '接受健康教育的人员类别';
COMMENT ON COLUMN "(群体)活动记录"."接受教育人数" IS '接受教育的人员数量';
COMMENT ON COLUMN "(群体)活动记录"."资料发放种类及数量" IS '健康宣教相关资料发放的种类及数量描述';
COMMENT ON COLUMN "(群体)活动记录"."活动内容" IS '医护人员组织开展群体活动的详细描述';
COMMENT ON COLUMN "(群体)活动记录"."活动总结评价" IS '健康宣教活动的总结与评价描述';
COMMENT ON TABLE "(群体)活动记录" IS '群体活动记录，包括活动机构、日期、地点、形式、主题、组织人员、接受教育人数、活动内容等';


CREATE TABLE IF NOT EXISTS "(
糖尿病)患者管理卡" ("学历代码" varchar (5) DEFAULT NULL,
 "婚姻状况代码" varchar (2) DEFAULT NULL,
 "婚姻状况名称" varchar (50) DEFAULT NULL,
 "医疗费用支付方式代码" varchar (30) DEFAULT NULL,
 "医疗费用支付方式名称" varchar (100) DEFAULT NULL,
 "医疗费用支付方式其他" varchar (100) DEFAULT NULL,
 "手机号码" varchar (20) DEFAULT NULL,
 "联系电话号码" varchar (20) DEFAULT NULL,
 "工作单位名称" varchar (70) DEFAULT NULL,
 "户籍地-详细地址" varchar (200) DEFAULT NULL,
 "户籍地-行政区划代码" varchar (12) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "户籍地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "户籍地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "户籍地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "户籍地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "户籍地-居委代码" varchar (12) DEFAULT NULL,
 "户籍地-居委名称" varchar (32) DEFAULT NULL,
 "户籍地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "户籍地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "户籍地-门牌号码" varchar (100) DEFAULT NULL,
 "户籍地-邮政编码" varchar (6) DEFAULT NULL,
 "居住地-详细地址" varchar (200) DEFAULT NULL,
 "居住地-行政区划代码" varchar (12) DEFAULT NULL,
 "居住地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "居住地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "居住地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "居住地-市(地区、州)名称" varchar (32) DEFAULT NULL,
 "居住地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "居住地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "居住地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "居住地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "居住地-居委代码" varchar (12) DEFAULT NULL,
 "居住地-居委名称" varchar (32) DEFAULT NULL,
 "居住地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "居住地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "居住地-门牌号码" varchar (70) DEFAULT NULL,
 "居住地-邮政编码" varchar (6) DEFAULT NULL,
 "表单编号" varchar (20) DEFAULT NULL,
 "建卡日期" date DEFAULT NULL,
 "建卡机构代码" varchar (22) DEFAULT NULL,
 "建卡机构名称" varchar (70) DEFAULT NULL,
 "建卡医生工号" varchar (20) DEFAULT NULL,
 "建卡医生姓名" varchar (50) DEFAULT NULL,
 "业务管理机构代码" varchar (22) DEFAULT NULL,
 "业务管理机构名称" varchar (70) DEFAULT NULL,
 "病例来源代码" varchar (2) DEFAULT NULL,
 "病例来源名称" varchar (50) DEFAULT NULL,
 "分型" varchar (50) DEFAULT NULL,
 "确诊方式" varchar (50) DEFAULT NULL,
 "确诊时并发症高血压情况" varchar (50) DEFAULT NULL,
 "确诊时并发症视网膜突变情况" varchar (50) DEFAULT NULL,
 "确诊时并发症糖尿病足情况" varchar (50) DEFAULT NULL,
 "确诊时并发症糖尿病肾病情况" varchar (50) DEFAULT NULL,
 "确诊时并发症糖尿病神经病变情况" varchar (50) DEFAULT NULL,
 "确诊时并发症高血脂情况" varchar (50) DEFAULT NULL,
 "确诊时并发症脑卒中情况" varchar (50) DEFAULT NULL,
 "确诊年份" decimal (4,
 0) DEFAULT NULL,
 "确诊医院" varchar (70) DEFAULT NULL,
 "家族史" varchar (100) DEFAULT NULL,
 "吸咽情况代码" varchar (2) DEFAULT NULL,
 "吸咽情况名称" varchar (50) DEFAULT NULL,
 "饮酒情况代码" varchar (2) DEFAULT NULL,
 "饮酒情况名称" varchar (50) DEFAULT NULL,
 "体育锻炼代码" varchar (2) DEFAULT NULL,
 "体育锻炼名称" varchar (50) DEFAULT NULL,
 "饮食习惯代码" varchar (2) DEFAULT NULL,
 "饮食习惯名称" varchar (50) DEFAULT NULL,
 "心理状况代码" varchar (2) DEFAULT NULL,
 "心理状况名称" varchar (50) DEFAULT NULL,
 "身高(cm)" decimal (4,
 1) DEFAULT NULL,
 "体重(kg)" decimal (4,
 1) DEFAULT NULL,
 "体质指数" decimal (4,
 2) DEFAULT NULL,
 "腰围(cm)" decimal (4,
 1) DEFAULT NULL,
 "臀围(cm)" decimal (4,
 1) DEFAULT NULL,
 "腰臀围比" decimal (2,
 1) DEFAULT NULL,
 "心率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "空腹血糖(mmol/L)" decimal (3,
 1) DEFAULT NULL,
 "餐后血糖(mmol/L)" decimal (3,
 1) DEFAULT NULL,
 "随机血糖(mmol/L)" decimal (3,
 1) DEFAULT NULL,
 "糖化血红蛋白值(%)" decimal (3,
 1) DEFAULT NULL,
 "生活自理能力代码" varchar (2) DEFAULT NULL,
 "生活自理能力名称" varchar (50) DEFAULT NULL,
 "终止管理标志" varchar (1) DEFAULT NULL,
 "终止日期" date DEFAULT NULL,
 "终止理由代码" varchar (2) DEFAULT NULL,
 "终止理由名称" varchar (50) DEFAULT NULL,
 "终止理由其他" varchar (100) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "学历名称" varchar (50) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "管理卡标识号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "民族代码" varchar (2) DEFAULT NULL,
 "民族名称" varchar (50) DEFAULT NULL,
 "职业类别代码" varchar (4) DEFAULT NULL,
 "职业类别名称" varchar (60) DEFAULT NULL,
 CONSTRAINT "(糖尿病)患者管理卡"_"医疗机构代码"_"管理卡标识号"_PK PRIMARY KEY ("医疗机构代码",
 "管理卡标识号")
);
COMMENT ON COLUMN "(糖尿病)患者管理卡"."职业类别名称" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."职业类别代码" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."民族名称" IS '所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."姓名" IS '本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."管理卡标识号" IS '按照某一特定编码规则赋予管理卡的唯一标识号';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."学历名称" IS '个体受教育最高程度的类别标准名称，如研究生教育、大学本科、专科教育等';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."终止理由其他" IS '患者终止管理的其他原因描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."终止理由名称" IS '患者终止管理的原因类别在特定编码体系中的名称，如死亡、迁出、失访等';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."终止理由代码" IS '患者终止管理的原因类别在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."终止日期" IS '高血压患者终止管理当日的公元纪年日期';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."终止管理标志" IS '标识该高血压患者是否终止管理';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."生活自理能力名称" IS '自己基本生活照料能力在特定编码体系中的名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."生活自理能力代码" IS '自己基本生活照料能力在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."糖化血红蛋白值(%)" IS '血液中糖化血红蛋白的测量值，计量单位为MMOL/L';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."随机血糖(mmol/L)" IS '任意时刻血液中葡萄糖定量检测结果值';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."餐后血糖(mmol/L)" IS '餐后血液中葡萄糖定量检测结果值，通常指餐后2小时的血糖值';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."空腹血糖(mmol/L)" IS '空腹状态下血液中葡萄糖定量检测结果值';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."心率(次/min)" IS '心脏搏动频率的测量值,计量单位为次/min';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."腰臀围比" IS '腰围与臀围的比值';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."臀围(cm)" IS '受检者臀部周长的测量值，计量单位为cm';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."腰围(cm)" IS '腰围测量值,计量单位为cm';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."体质指数" IS '根据体重(kg)除以身高平方(m2)计算出的指数';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."身高(cm)" IS '个体身高的测量值，计量单位为cm';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."心理状况名称" IS '个体心理状况的分类在特定编码体系中的名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."心理状况代码" IS '个体心理状况的分类在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."饮食习惯名称" IS '个体饮食习惯的标准名称，如荤素均衡、荤食为主、素食为主、嗜盐、嗜油等';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."饮食习惯代码" IS '个体饮食习惯在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."体育锻炼名称" IS '个体最近1个月主动运动的频率的标准名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."体育锻炼代码" IS '个体最近1个月主动运动的频率在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."饮酒情况名称" IS '患者饮酒频率分类的标准名称，如从不、偶尔、少于1d/月等';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."饮酒情况代码" IS '患者饮酒频率分类(如从不、偶尔、少于1d/月等)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."吸咽情况名称" IS '个体现在吸烟频率的标准名称，如现在每天吸；现在吸，但不是每天吸；过去吸，现在不吸；从不吸';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."吸咽情况代码" IS '个体现在吸烟频率(如现在每天吸；现在吸，但不是每天吸；过去吸，现在不吸；从不吸)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."家族史" IS '患者3代以内有血缘关系的家族成员中所患遗传疾病史的描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."确诊医院" IS '确诊医院的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."确诊年份" IS '糖尿病确诊是的公元年份';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."确诊时并发症脑卒中情况" IS '患者确诊时并发脑卒中的情况描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."确诊时并发症高血脂情况" IS '患者确诊时并发高血脂的情况描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."确诊时并发症糖尿病神经病变情况" IS '患者确诊时并发糖尿病神经病变的情况描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."确诊时并发症糖尿病肾病情况" IS '患者确诊时并发糖尿病肾病的情况描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."确诊时并发症糖尿病足情况" IS '患者确诊时并发糖尿病足的情况描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."确诊时并发症视网膜突变情况" IS '患者确诊时并发视网膜突的情况描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."确诊时并发症高血压情况" IS '患者确诊时并发高血压的情况描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."确诊方式" IS '对个体作出疾病诊断所采用的方法的详细描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."分型" IS '病例分型的名称及描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."病例来源名称" IS '病例来源在特定编码体系中的名称，如门诊就诊、健康档案、首诊测压等';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."病例来源代码" IS '病例来源在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."业务管理机构名称" IS '业务管理机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."业务管理机构代码" IS '按照机构内编码规则赋予业务管理机构的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."建卡医生姓名" IS '建卡医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."建卡医生工号" IS '建卡医师的工号';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."建卡机构名称" IS '建卡机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."建卡机构代码" IS '按照某一特定编码规则赋予建卡机构的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."建卡日期" IS '完成建卡时的公元纪年日期';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."表单编号" IS '按照某一特定规则赋予表单的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-邮政编码" IS '现住地址中所在行政区的邮政编码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-门牌号码" IS '本人现住地址中的门牌号码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-村(街、路、弄等)名称" IS '本人现住地址中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-村(街、路、弄等)代码" IS '本人现住地址中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-居委名称" IS '现住地址所属的居民委员会名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-居委代码" IS '现住地址所属的居民委员会在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-乡(镇、街道办事处)名称" IS '本人现住地址中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-乡(镇、街道办事处)代码" IS '本人现住地址中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-县(市、区)名称" IS '现住地址中的县或区名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-县(市、区)代码" IS '现住地址中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-市(地区、州)名称" IS '现住地址中的市、地区或州的名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-市(地区、州)代码" IS '现住地址中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-省(自治区、直辖市)名称" IS '现住地址中的省、自治区或直辖市名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-省(自治区、直辖市)代码" IS '现住地址中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-行政区划代码" IS '居住地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."居住地-详细地址" IS '现住地址的详细描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-邮政编码" IS '户籍地址所在行政区的邮政编码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-门牌号码" IS '户籍登记所在地址的门牌号码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-村(街、路、弄等)名称" IS '户籍登记所在地址的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-村(街、路、弄等)代码" IS '户籍登记所在地址的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-居委名称" IS '户籍登记所在地址的居民委员会名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-居委代码" IS '户籍登记所在地址的居民委员会在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-乡(镇、街道办事处)名称" IS '户籍登记所在地址的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-乡(镇、街道办事处)代码" IS '户籍登记所在地址的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-县(市、区)名称" IS '户籍登记所在地址的县(市、区)的名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-县(市、区)代码" IS '户籍登记所在地址的县(区)的在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-市(地区、州)名称" IS '户籍登记所在地址的市、地区或州的名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-市(地区、州)代码" IS '户籍登记所在地址的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-省(自治区、直辖市)名称" IS '户籍登记所在地址的省、自治区或直辖市名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-省(自治区、直辖市)代码" IS '户籍登记所在地址的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-行政区划代码" IS '户籍地址所在区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."户籍地-详细地址" IS '户籍地址的详细描述';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."工作单位名称" IS '个体工作单位的组织机构名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."联系电话号码" IS '居民本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."手机号码" IS '居民本人的手机号码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."医疗费用支付方式其他" IS '除上述医疗费用支付方式外，其他支付方式的名称';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."医疗费用支付方式名称" IS '患者医疗费用支付方式类别在特定编码体系中的名称，如城镇职工基本医疗保险、城镇居民基本医疗保险等';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."医疗费用支付方式代码" IS '患者医疗费用支付方式类别(如城镇职工基本医疗保险、城镇居民基本医疗保险等)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."婚姻状况名称" IS '当前婚姻状况的标准名称，如已婚、未婚、初婚等';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."婚姻状况代码" IS '当前婚姻状况(已婚、未婚、初婚等)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理卡"."学历代码" IS '个体受教育最高程度的类别(如研究生教育、大学本科、专科教育等)在特定编码体系中的代码';
COMMENT ON TABLE "(糖尿病)患者管理卡" IS '糖尿病患者基本信息，包括人口统计学信息，建卡信息、并发症情况、生活方式等';


CREATE TABLE IF NOT EXISTS "(
精神病)患者用药记录" ("修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "用药流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "补充表标识号" varchar (64) DEFAULT NULL,
 "随访流水号" varchar (64) DEFAULT NULL,
 "中药类别代码" varchar (2) DEFAULT NULL,
 "中药类别名称" varchar (50) DEFAULT NULL,
 "药物名称" varchar (50) DEFAULT NULL,
 "药物使用频率名称" varchar (20) DEFAULT NULL,
 "药物使用剂量单位" varchar (6) DEFAULT NULL,
 "药物使用次剂量" decimal (4,
 2) DEFAULT NULL,
 "药物使用总剂量" decimal (11,
 2) DEFAULT NULL,
 "药物使用途径代码" varchar (32) DEFAULT NULL,
 "药物使用途径名称" varchar (50) DEFAULT NULL,
 "用药开始时间" timestamp DEFAULT NULL,
 "用药停止时间" timestamp DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 CONSTRAINT "(精神病)患者用药记录"_"医疗机构代码"_"用药流水号"_PK PRIMARY KEY ("医疗机构代码",
 "用药流水号")
);
COMMENT ON COLUMN "(精神病)患者用药记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(精神病)患者用药记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(精神病)患者用药记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(精神病)患者用药记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(精神病)患者用药记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(精神病)患者用药记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(精神病)患者用药记录"."用药停止时间" IS '用药结束时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(精神病)患者用药记录"."用药开始时间" IS '用药开始时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(精神病)患者用药记录"."药物使用途径名称" IS '药物的给药途径在特定编码体系中的名称，如口服、静滴、喷喉等';
COMMENT ON COLUMN "(精神病)患者用药记录"."药物使用途径代码" IS '药物使用途径(如口服、静滴、喷喉等)在特定编码体系中的代码';
COMMENT ON COLUMN "(精神病)患者用药记录"."药物使用总剂量" IS '在一定时间段内使用药物的总量。根据单次剂量、频次计算得到的总量';
COMMENT ON COLUMN "(精神病)患者用药记录"."药物使用次剂量" IS '单次使用药物的剂量，按剂量单位计';
COMMENT ON COLUMN "(精神病)患者用药记录"."药物使用剂量单位" IS '药物使用剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "(精神病)患者用药记录"."药物使用频率名称" IS '单位时间内药物使用频次类别的标准名称，如每天两次、每周两次、睡前一次等';
COMMENT ON COLUMN "(精神病)患者用药记录"."药物名称" IS '药品在特定编码体系中的名称。对于医疗机构制剂，此处填写医疗机构制剂名称';
COMMENT ON COLUMN "(精神病)患者用药记录"."中药类别名称" IS '中药使用类别的标准名称，如未使用、中成药、中草药、其他中药等';
COMMENT ON COLUMN "(精神病)患者用药记录"."中药类别代码" IS '中药使用类别(如未使用、中成药、中草药、其他中药)在特定编码体系中的代码';
COMMENT ON COLUMN "(精神病)患者用药记录"."随访流水号" IS '按照特定编码规则赋予用药指导记录唯一标志的顺序号';
COMMENT ON COLUMN "(精神病)患者用药记录"."补充表标识号" IS '按照某一特定规则赋予补充表的唯一标识';
COMMENT ON COLUMN "(精神病)患者用药记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(精神病)患者用药记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(精神病)患者用药记录"."用药流水号" IS '按照一定编码规则赋予产后访视记录的顺序号';
COMMENT ON COLUMN "(精神病)患者用药记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(精神病)患者用药记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(精神病)患者用药记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(精神病)患者用药记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(精神病)患者用药记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(精神病)患者用药记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(精神病)患者用药记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(精神病)患者用药记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(精神病)患者用药记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON TABLE "(精神病)患者用药记录" IS '精神病患者用药记录，包括药物类别、名称、使用频率、剂量、途径等';


CREATE TABLE IF NOT EXISTS "(
签约)签约记录" ("登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "解约原因其他" varchar (100) DEFAULT NULL,
 "解约原因名称" varchar (50) DEFAULT NULL,
 "解约原因代码" varchar (2) DEFAULT NULL,
 "解约日期" date DEFAULT NULL,
 "重点人群类型" varchar (50) DEFAULT NULL,
 "重点人群标志" varchar (1) DEFAULT NULL,
 "签约生效日期" date DEFAULT NULL,
 "签约日期" date DEFAULT NULL,
 "签约状态名称" varchar (50) DEFAULT NULL,
 "签约状态代码" varchar (2) DEFAULT NULL,
 "签约医生姓名" varchar (50) DEFAULT NULL,
 "签约医生工号" varchar (20) DEFAULT NULL,
 "签约团队名称" varchar (100) DEFAULT NULL,
 "签约团队代码" varchar (64) DEFAULT NULL,
 "家庭标识号" varchar (64) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人签约流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "有效期截止日期" date DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "(签约)签约记录"_"个人签约流水号"_"医疗机构代码"_PK PRIMARY KEY ("个人签约流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(签约)签约记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(签约)签约记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(签约)签约记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(签约)签约记录"."有效期截止日期" IS '家庭医生服务有效的截止日期';
COMMENT ON COLUMN "(签约)签约记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(签约)签约记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(签约)签约记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(签约)签约记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(签约)签约记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(签约)签约记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(签约)签约记录"."个人签约流水号" IS '按照某一特性编码规则赋予个人家庭医生签约记录的顺序号';
COMMENT ON COLUMN "(签约)签约记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(签约)签约记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(签约)签约记录"."家庭标识号" IS '按照特定编码规则赋予家庭的唯一标识';
COMMENT ON COLUMN "(签约)签约记录"."签约团队代码" IS '签约团队在家庭医生签约团队信息表中对应的代码';
COMMENT ON COLUMN "(签约)签约记录"."签约团队名称" IS '签约团队的名称描述';
COMMENT ON COLUMN "(签约)签约记录"."签约医生工号" IS '签约医师的工号';
COMMENT ON COLUMN "(签约)签约记录"."签约医生姓名" IS '签约医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(签约)签约记录"."签约状态代码" IS '签约状态(如新签、解约、续签、转签等)在特定编码体系中的代码';
COMMENT ON COLUMN "(签约)签约记录"."签约状态名称" IS '签约状态(如新签、解约、续签、转签等)在特定编码体系中的名称';
COMMENT ON COLUMN "(签约)签约记录"."签约日期" IS '完成签约时的公元纪年日期';
COMMENT ON COLUMN "(签约)签约记录"."签约生效日期" IS '签约后家庭医生服务开始生效当日的公元纪年日期';
COMMENT ON COLUMN "(签约)签约记录"."重点人群标志" IS '标识签约对象是否为重点人群的标志';
COMMENT ON COLUMN "(签约)签约记录"."重点人群类型" IS '重点人群类型(如老年人、孕产妇、儿童、残疾人等)在特定编码体系中的代码';
COMMENT ON COLUMN "(签约)签约记录"."解约日期" IS '家庭医生解约时的公元纪年日期';
COMMENT ON COLUMN "(签约)签约记录"."解约原因代码" IS '解约原因(如死亡、迁出、不满意、医生换岗等)在标准定编码体系中的代码';
COMMENT ON COLUMN "(签约)签约记录"."解约原因名称" IS '解约原因(如死亡、迁出、不满意、医生换岗等)在特定编码体系中的名称';
COMMENT ON COLUMN "(签约)签约记录"."解约原因其他" IS '除上述原因外，其他解约原因名称';
COMMENT ON COLUMN "(签约)签约记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(签约)签约记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON TABLE "(签约)签约记录" IS '家庭医生签约记录，包括签约团队、签约医生、重点人群标志等';


CREATE TABLE IF NOT EXISTS "(
签约)签约服务包" ("居民自付金额" decimal (12,
 2) DEFAULT NULL,
 "财政投入金额" decimal (12,
 2) DEFAULT NULL,
 "公共卫生服务经费金额" decimal (12,
 2) DEFAULT NULL,
 "基层医疗机构减免金额" decimal (12,
 2) DEFAULT NULL,
 "优惠金额" decimal (18,
 3) DEFAULT NULL,
 "其他补助金额" decimal (12,
 2) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医保基金金额" decimal (10,
 2) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "服务代码" varchar (50) NOT NULL,
 "服务名称" varchar (200) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "服务包内容" text,
 "适用对象" varchar (100) DEFAULT NULL,
 "收费标准" decimal (12,
 2) DEFAULT NULL,
 "政策年度" decimal (4,
 0) DEFAULT NULL,
 "总金额" decimal (12,
 2) DEFAULT NULL,
 CONSTRAINT "(签约)签约服务包"_"医疗机构代码"_"服务代码"_PK PRIMARY KEY ("医疗机构代码",
 "服务代码")
);
COMMENT ON COLUMN "(签约)签约服务包"."总金额" IS '家庭医生签约服务包的收费总金额，计量单位为人民币元';
COMMENT ON COLUMN "(签约)签约服务包"."政策年度" IS '签约服务政策所适用的年份';
COMMENT ON COLUMN "(签约)签约服务包"."收费标准" IS '家庭医生签约服务包的收费标准金额，计量单位为人民币元';
COMMENT ON COLUMN "(签约)签约服务包"."适用对象" IS '适用对象的描述，如高危人群、老年人等';
COMMENT ON COLUMN "(签约)签约服务包"."服务包内容" IS '家庭医生签约服务包的内容描述';
COMMENT ON COLUMN "(签约)签约服务包"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(签约)签约服务包"."服务名称" IS '家庭医生服务的名称';
COMMENT ON COLUMN "(签约)签约服务包"."服务代码" IS '家庭医生服务在特定编码体系中的代码';
COMMENT ON COLUMN "(签约)签约服务包"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(签约)签约服务包"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(签约)签约服务包"."医保基金金额" IS '家庭医生签约服务包的医保基金支付部分的金额，计量单位为人民币元';
COMMENT ON COLUMN "(签约)签约服务包"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约服务包"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约服务包"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(签约)签约服务包"."其他补助金额" IS '家庭医生签约服务包的其他补助支付部分的金额，计量单位为人民币元';
COMMENT ON COLUMN "(签约)签约服务包"."优惠金额" IS '家庭医生签约服务包的优惠部分的金额，计量单位为人民币元';
COMMENT ON COLUMN "(签约)签约服务包"."基层医疗机构减免金额" IS '家庭医生签约服务包的基层医疗机构减免部分支付的金额，计量单位为人民币元';
COMMENT ON COLUMN "(签约)签约服务包"."公共卫生服务经费金额" IS '家庭医生签约服务包的公共卫生服务经费支付部分的金额，计量单位为人民币元';
COMMENT ON COLUMN "(签约)签约服务包"."财政投入金额" IS '家庭医生签约服务包的财政投入部分的金额，计量单位为人民币元';
COMMENT ON COLUMN "(签约)签约服务包"."居民自付金额" IS '家庭医生签约服务包的居民自付部分的金额，计量单位为人民币元';
COMMENT ON TABLE "(签约)签约服务包" IS '家庭医生签约服务包信息，包括服务名称、服务包内容、使用对象、收费标准、政策年度等';


CREATE TABLE IF NOT EXISTS "(
签约)签约医生" ("修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医生编号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "医生工号" varchar (20) DEFAULT NULL,
 "医生姓名" varchar (50) DEFAULT NULL,
 "团队代码" varchar (64) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "身份证号" varchar (18) DEFAULT NULL,
 "联系电话号码" varchar (30) DEFAULT NULL,
 "资格级别" varchar (20) DEFAULT NULL,
 "资格证书编号" varchar (50) DEFAULT NULL,
 "职业证书编号" varchar (50) DEFAULT NULL,
 "专业特长" varchar (200) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 CONSTRAINT "(签约)签约医生"_"医疗机构代码"_"医生编号"_PK PRIMARY KEY ("医疗机构代码",
 "医生编号")
);
COMMENT ON COLUMN "(签约)签约医生"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(签约)签约医生"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(签约)签约医生"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(签约)签约医生"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约医生"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(签约)签约医生"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(签约)签约医生"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(签约)签约医生"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(签约)签约医生"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约医生"."专业特长" IS '医生的专业特长';
COMMENT ON COLUMN "(签约)签约医生"."职业证书编号" IS '个人职业证书上的证书编码';
COMMENT ON COLUMN "(签约)签约医生"."资格证书编号" IS '个人资格证书上的证书编码';
COMMENT ON COLUMN "(签约)签约医生"."资格级别" IS '个人职业资质对应的级别，如正高、副高、一级、二级等';
COMMENT ON COLUMN "(签约)签约医生"."联系电话号码" IS '本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(签约)签约医生"."身份证号" IS '个体居民身份证上的唯一法定标识符';
COMMENT ON COLUMN "(签约)签约医生"."出生日期" IS '员工出生当日的公元纪年日期';
COMMENT ON COLUMN "(签约)签约医生"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "(签约)签约医生"."团队代码" IS '签约团队在家庭医生签约团队信息表中对应的代码';
COMMENT ON COLUMN "(签约)签约医生"."医生姓名" IS '医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(签约)签约医生"."医生工号" IS '医师的工号';
COMMENT ON COLUMN "(签约)签约医生"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(签约)签约医生"."医生编号" IS '医师在机构内的唯一标识';
COMMENT ON COLUMN "(签约)签约医生"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(签约)签约医生"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(签约)签约医生"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约医生"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约医生"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(签约)签约医生"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON TABLE "(签约)签约医生" IS '家庭医生签约医生信息，包括医生姓名、性别、身份证号、执业资质、专业特长等';


CREATE TABLE IF NOT EXISTS "(
新生儿疾病筛查)筛查情况" ("修改标志" varchar (1) DEFAULT NULL,
 "标本编号" varchar (20) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "筛查结果" varchar (100) DEFAULT NULL,
 "筛查方法其他" varchar (100) DEFAULT NULL,
 "筛查方法名称" varchar (50) DEFAULT NULL,
 "筛查方法代码" varchar (2) DEFAULT NULL,
 "筛查项目其他" varchar (100) DEFAULT NULL,
 "筛查项目名称" varchar (100) DEFAULT NULL,
 "筛查项目代码" varchar (30) DEFAULT NULL,
 "筛查人员姓名" varchar (50) DEFAULT NULL,
 "筛查机构名称" varchar (70) DEFAULT NULL,
 "筛查机构代码" varchar (22) DEFAULT NULL,
 "筛查时间" timestamp DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 CONSTRAINT "(新生儿疾病筛查)筛查情况"_"标本编号"_"医疗机构代码"_PK PRIMARY KEY ("标本编号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."筛查时间" IS '筛查当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."筛查机构代码" IS '按照某一特定编码规则赋予筛查医疗机构的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."筛查机构名称" IS '筛查机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."筛查人员姓名" IS '筛查人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."筛查项目代码" IS '新生儿疾病筛查的项目在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."筛查项目名称" IS '新生儿疾病筛查的项目在特定编码体系中的名称';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."筛查项目其他" IS '新生儿其他疾病筛查项目的描述';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."筛查方法代码" IS '新生儿疾病筛查方法所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."筛查方法名称" IS '新生儿疾病筛查方法所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."筛查方法其他" IS '新生儿疾病其他筛查方法描述';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."筛查结果" IS '新生儿疾病筛查结果的分类代码';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."标本编号" IS '按照某一特定编码规则赋予检查标本的顺序号';
COMMENT ON COLUMN "(新生儿疾病筛查)筛查情况"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON TABLE "(新生儿疾病筛查)筛查情况" IS '新生儿疾病筛查情况，包括筛查方法、项目、结果等';


CREATE TABLE IF NOT EXISTS "(
新生儿疾病筛查)可疑阳性召回情况" ("通知形式代码" varchar (2) DEFAULT NULL,
 "通知形式名称" varchar (50) DEFAULT NULL,
 "通知形式其他" varchar (100) DEFAULT NULL,
 "通知到达人姓名" varchar (50) DEFAULT NULL,
 "通知到达人与新生儿关系代码" varchar (2) DEFAULT NULL,
 "通知到达人与新生儿关系名称" varchar (50) DEFAULT NULL,
 "召回时间" timestamp DEFAULT NULL,
 "未召回原因" varchar (100) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "标本编号" varchar (20) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "通知时间" timestamp DEFAULT NULL,
 "通知机构代码" varchar (22) DEFAULT NULL,
 "通知机构名称" varchar (70) DEFAULT NULL,
 "通知人员工号" varchar (20) DEFAULT NULL,
 "通知人员姓名" varchar (50) DEFAULT NULL,
 CONSTRAINT "(新生儿疾病筛查)可疑阳性召回情况"_"医疗机构代码"_"标本编号"_PK PRIMARY KEY ("医疗机构代码",
 "标本编号")
);
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."通知人员姓名" IS '通知人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."通知人员工号" IS '通知人员的工号';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."通知机构名称" IS '可疑阳性召回通知机构的组织机构名称';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."通知机构代码" IS '可疑阳性召回通知机构的组织机构代码';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."通知时间" IS '可疑阳性召回通知当日的公元纪年日期和时间的详细描述';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."标本编号" IS '按照某一特定编码规则赋予检查标本的顺序号';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."未召回原因" IS '未能召回原因的详细描述';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."召回时间" IS '可疑阳性召回当日的公元纪年日期和时间的详细描述';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."通知到达人与新生儿关系名称" IS '通知到达人与新生儿关系类别(如配偶、子女、父母等)在特定编码体系中的名称';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."通知到达人与新生儿关系代码" IS '通知到达人与新生儿关系类别(如配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."通知到达人姓名" IS '通知到达人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."通知形式其他" IS '可疑阳性召回其他通知形式描述';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."通知形式名称" IS '通知形式在特定编码体系中的代码，如自取、电话、函件等';
COMMENT ON COLUMN "(新生儿疾病筛查)可疑阳性召回情况"."通知形式代码" IS '通知形式在特定编码体系中的代码';
COMMENT ON TABLE "(新生儿疾病筛查)可疑阳性召回情况" IS '新生儿疾病可疑阳性召回情况，包括通知形式、通知到达人、通知时间等';


CREATE TABLE IF NOT EXISTS "(
新冠肺炎)随访记录" ("个人唯一标识号" varchar (64) DEFAULT NULL,
 "随访方式代码" varchar (2) DEFAULT NULL,
 "随访方式名称" varchar (50) DEFAULT NULL,
 "随访方式其他" varchar (100) DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "咳嗽标志" varchar (1) DEFAULT NULL,
 "气促标志" varchar (1) DEFAULT NULL,
 "处置措施" varchar (1) DEFAULT NULL,
 "随访医生工号" varchar (20) DEFAULT NULL,
 "随访医生姓名" varchar (50) DEFAULT NULL,
 "随访日期" date DEFAULT NULL,
 "下次随访日期" date DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "随访流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "专项档案标识号" varchar (64) DEFAULT NULL,
 CONSTRAINT "(新冠肺炎)随访记录"_"医疗机构代码"_"随访流水号"_PK PRIMARY KEY ("医疗机构代码",
 "随访流水号")
);
COMMENT ON COLUMN "(新冠肺炎)随访记录"."专项档案标识号" IS '按照某一特定规则赋予专项档案的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."随访流水号" IS '按照一定编码规则赋予产后访视记录的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."下次随访日期" IS '下次对患者进行随访时当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."随访日期" IS '对患者进行随访时当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."随访医生姓名" IS '随访医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."随访医生工号" IS '随访医师的工号';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."处置措施" IS '出现症状采取的主要措施,包含中医及民族医相关治疗措施';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."气促标志" IS '标识患者是否出现气促情况的标志';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."咳嗽标志" IS '标识患者是否咳嗽的标志';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."随访方式其他" IS '除上述症状外，其他随访方式的详细描述';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."随访方式名称" IS '随访方式的特定类型在特定编码体系中的名称';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."随访方式代码" IS '随访方式的特定类型在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)随访记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON TABLE "(新冠肺炎)随访记录" IS '新冠肺炎随访记录，包括随访方式、体温、咳嗽、气促、处置措施等';


CREATE TABLE IF NOT EXISTS "(
新冠肺炎)调查表" ("出生日期" date DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "调查表流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "户籍地址" varchar (200) DEFAULT NULL,
 "户籍地址行政区划代码" varchar (18) DEFAULT NULL,
 "居住地址" varchar (200) DEFAULT NULL,
 "居住地-行政区划代码" varchar (18) DEFAULT NULL,
 "管理所在行政区划代码" varchar (18) DEFAULT NULL,
 "管理所在行政区划名称" varchar (200) DEFAULT NULL,
 "人员状态" varchar (1) DEFAULT NULL,
 "处置措施代码" varchar (1) DEFAULT NULL,
 "处置措施名称" varchar (20) DEFAULT NULL,
 "调查医生工号" varchar (20) DEFAULT NULL,
 "调查医生姓名" varchar (50) DEFAULT NULL,
 "调查日期" date DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "健康指导" text,
 "联系电话号码" varchar (20) DEFAULT NULL,
 "家属姓名" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 CONSTRAINT "(新冠肺炎)调查表"_"调查表流水号"_"医疗机构代码"_PK PRIMARY KEY ("调查表流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(新冠肺炎)调查表"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)调查表"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(新冠肺炎)调查表"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "(新冠肺炎)调查表"."家属姓名" IS '家属在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)调查表"."联系电话号码" IS '患者本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(新冠肺炎)调查表"."健康指导" IS '描述医师针对病人情况而提出的指导建议';
COMMENT ON COLUMN "(新冠肺炎)调查表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)调查表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)调查表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(新冠肺炎)调查表"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)调查表"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查表"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "(新冠肺炎)调查表"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)调查表"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(新冠肺炎)调查表"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)调查表"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)调查表"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查表"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)调查表"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(新冠肺炎)调查表"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)调查表"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "(新冠肺炎)调查表"."调查日期" IS '开展调查时的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)调查表"."调查医生姓名" IS '调查医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)调查表"."调查医生工号" IS '调查医师的工号';
COMMENT ON COLUMN "(新冠肺炎)调查表"."处置措施名称" IS '新冠肺炎处置措施的分类在特定编码体系中的名称';
COMMENT ON COLUMN "(新冠肺炎)调查表"."处置措施代码" IS '新冠肺炎处置措施的分类在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)调查表"."人员状态" IS '新冠肺炎人员状态的分类在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)调查表"."管理所在行政区划名称" IS '管理所在行政区划的详细描述';
COMMENT ON COLUMN "(新冠肺炎)调查表"."管理所在行政区划代码" IS '管理所在行政区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查表"."居住地-行政区划代码" IS '居住地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查表"."居住地址" IS '现住地址的详细描述';
COMMENT ON COLUMN "(新冠肺炎)调查表"."户籍地址行政区划代码" IS '户籍地址所在区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查表"."户籍地址" IS '户籍地址的详细描述';
COMMENT ON COLUMN "(新冠肺炎)调查表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查表"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新冠肺炎)调查表"."调查表流水号" IS '按照某一特定编码规则赋予调查表的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(新冠肺炎)调查表"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查表"."姓名" IS '本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)调查表"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)调查表"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON TABLE "(新冠肺炎)调查表" IS '新冠肺炎调查内容，包括个人基本信息、管理所在行政区划、人员状态、处置措施、体温、健康指导等';


CREATE TABLE IF NOT EXISTS "(
新冠肺炎)核酸检测信息" ("医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "上报结果名称" varchar (50) DEFAULT NULL,
 "检测结果代码" varchar (2) DEFAULT NULL,
 "上报结果代码" varchar (2) DEFAULT NULL,
 "检测机构" varchar (64) DEFAULT NULL,
 "检测结果名称" varchar (50) DEFAULT NULL,
 "第几次检测" decimal (32,
 0) DEFAULT NULL,
 "身份证号" varchar (18) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "委托单位名称" varchar (64) DEFAULT NULL,
 "病例类型代码" varchar (1) DEFAULT NULL,
 "采样日期" date DEFAULT NULL,
 "样本来源" varchar (70) DEFAULT NULL,
 "填报单位" varchar (70) DEFAULT NULL,
 "送检编号" varchar (50) DEFAULT NULL,
 "检测日期" date NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "检测流水号" varchar (64) NOT NULL,
 CONSTRAINT "(新冠肺炎)核酸检测信息"_"医疗机构代码"_"检测日期"_"检测流水号"_PK PRIMARY KEY ("医疗机构代码",
 "检测日期",
 "检测流水号")
);
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."检测流水号" IS '按照某一特定编码规则赋予检测记录的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."检测日期" IS '新冠核酸检测时的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."送检编号" IS '按照某一特定编码规则赋予新冠肺炎核酸检测样本送检的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."填报单位" IS '填报单位的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."样本来源" IS '新冠核酸检测样本来源的描述';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."采样日期" IS '采集标本时的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."病例类型代码" IS '新冠病例类型(如临床诊断、疑似、确诊等)在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."委托单位名称" IS '委托单位的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."姓名" IS '本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."身份证号" IS '个体居民身份证上的唯一法定标识符';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."第几次检测" IS '患者核酸检测的次数排序号';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."检测结果名称" IS '核酸检测结果(如阴性、阳性、样本不合格、可疑待查等)在特定编码体系中的名称';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."检测机构" IS '检测机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."上报结果代码" IS '核酸检测上报结果(如阴性、阳性、样本不合格、可疑待查等)在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."检测结果代码" IS '核酸检测结果(如阴性、阳性、样本不合格、可疑待查等)在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."上报结果名称" IS '核酸检测上报结果(如阴性、阳性、样本不合格、可疑待查等)在特定编码体系中的名称';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)核酸检测信息"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON TABLE "(新冠肺炎)核酸检测信息" IS '新冠肺炎核酸检测信息，包括采样时间、病历类型、个人基本信息、检测结果等';


CREATE TABLE IF NOT EXISTS "(
家庭)成员信息" ("与户主关系其他" varchar (50) DEFAULT NULL,
 "与户主关系名称" varchar (50) DEFAULT NULL,
 "与户主关系代码" varchar (2) DEFAULT NULL,
 "成员编号" varchar (2) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "家庭标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "成员流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 CONSTRAINT "(家庭)成员信息"_"成员流水号"_"医疗机构代码"_PK PRIMARY KEY ("成员流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(家庭)成员信息"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(家庭)成员信息"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(家庭)成员信息"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(家庭)成员信息"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(家庭)成员信息"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(家庭)成员信息"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(家庭)成员信息"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(家庭)成员信息"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(家庭)成员信息"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(家庭)成员信息"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(家庭)成员信息"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(家庭)成员信息"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(家庭)成员信息"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(家庭)成员信息"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(家庭)成员信息"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(家庭)成员信息"."成员流水号" IS '按照某一特性编码规则赋予个人家庭医生签约记录的顺序号';
COMMENT ON COLUMN "(家庭)成员信息"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(家庭)成员信息"."家庭标识号" IS '按照特定编码规则赋予家庭的唯一标识';
COMMENT ON COLUMN "(家庭)成员信息"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(家庭)成员信息"."成员编号" IS '按照某一特定编码规则赋予家庭成员的顺序号';
COMMENT ON COLUMN "(家庭)成员信息"."与户主关系代码" IS '家庭成员与户主关系的家庭关系在特定编码体系中的代码';
COMMENT ON COLUMN "(家庭)成员信息"."与户主关系名称" IS '家庭成员与户主关系的标准名称,如配偶、子女、父母等';
COMMENT ON COLUMN "(家庭)成员信息"."与户主关系其他" IS '除上述家庭关系外，其他类型与户主关系的名称';
COMMENT ON TABLE "(家庭)成员信息" IS '家庭成员信息，包括成员编号、与户主关系等';


CREATE TABLE IF NOT EXISTS "(
孕产妇)高危孕妇" ("个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "孕册编号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "结案机构名称" varchar (70) DEFAULT NULL,
 "结案机构代码" varchar (22) DEFAULT NULL,
 "结案医生姓名" varchar (50) DEFAULT NULL,
 "结案医生工号" varchar (20) DEFAULT NULL,
 "结案日期" date DEFAULT NULL,
 "结案标志" varchar (1) DEFAULT NULL,
 "高危转归其他" varchar (50) DEFAULT NULL,
 "高危转归名称" varchar (50) DEFAULT NULL,
 "高危转归代码" varchar (2) DEFAULT NULL,
 "初次诊断日期" date DEFAULT NULL,
 CONSTRAINT "(孕产妇)高危孕妇"_"孕册编号"_"医疗机构代码"_PK PRIMARY KEY ("孕册编号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(孕产妇)高危孕妇"."初次诊断日期" IS '高危孕产妇初次诊断完成时的公元纪年日期';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."高危转归代码" IS '高危孕妇疾病治疗结果的类别(如治愈、好转、稳定、恶化等)在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."高危转归名称" IS '高危孕妇疾病治疗结果的类别(如治愈、好转、稳定、恶化等)在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."高危转归其他" IS '除上述类别外，其他高危孕产妇疾病转归的名称';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."结案标志" IS '标识孕册是否结案的标志';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."结案日期" IS '孕册结案时的公元纪年日期';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."结案医生工号" IS '结案医师的工号';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."结案医生姓名" IS '结案医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."结案机构代码" IS '结案机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."结案机构名称" IS '结案机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."孕册编号" IS '按照某一特定编码规则赋予孕产妇孕册的唯一标识';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(孕产妇)高危孕妇"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON TABLE "(孕产妇)高危孕妇" IS '孕妇高危妊娠登记的基本信息，如初次诊断日期、高危转归等';


CREATE TABLE IF NOT EXISTS "(
孕产妇)产后访视记录" ("医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "随访流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "孕册编号" varchar (64) NOT NULL,
 "随访日期" date DEFAULT NULL,
 "随访医生工号" varchar (20) DEFAULT NULL,
 "随访医生姓名" varchar (50) DEFAULT NULL,
 "随访机构代码" varchar (22) DEFAULT NULL,
 "随访机构名称" varchar (70) DEFAULT NULL,
 "分娩日期" date DEFAULT NULL,
 "出院日期" date DEFAULT NULL,
 "产后天数" decimal (2,
 0) DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "健康状况描述" varchar (100) DEFAULT NULL,
 "心理状况描述" varchar (100) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "乳房异常标志" varchar (1) DEFAULT NULL,
 "乳房异常描述" varchar (100) DEFAULT NULL,
 "左侧乳腺检查结果代码" varchar (30) DEFAULT NULL,
 "左侧乳腺检查结果名称" varchar (100) DEFAULT NULL,
 "左侧乳腺检查结果其他" varchar (100) DEFAULT NULL,
 "右侧乳腺检查结果代码" varchar (30) DEFAULT NULL,
 "右侧乳腺检查结果名称" varchar (100) DEFAULT NULL,
 "右侧乳腺检查结果其他" varchar (100) DEFAULT NULL,
 "恶露异常标志" varchar (1) DEFAULT NULL,
 "恶露状况" varchar (100) DEFAULT NULL,
 "子宫异常标志" varchar (1) DEFAULT NULL,
 "子宫异常描述" varchar (100) DEFAULT NULL,
 "伤口愈合状况代码" varchar (2) DEFAULT NULL,
 "伤口愈合状况名称" varchar (50) DEFAULT NULL,
 "伤口愈合状况其他" varchar (100) DEFAULT NULL,
 "其他检查结果" varchar (100) DEFAULT NULL,
 "健康评估异常标志" varchar (1) DEFAULT NULL,
 "健康评估异常结果描述" varchar (100) DEFAULT NULL,
 "健康保健指导代码" varchar (30) DEFAULT NULL,
 "健康保健指导名称" varchar (100) DEFAULT NULL,
 "健康保健指导其他" varchar (100) DEFAULT NULL,
 "转诊标志" varchar (1) DEFAULT NULL,
 "转诊原因" varchar (100) DEFAULT NULL,
 "转入医疗机构名称" varchar (70) DEFAULT NULL,
 "转入机构科室名称" varchar (100) DEFAULT NULL,
 "下次随访日期" date DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "(孕产妇)产后访视记录"_"医疗机构代码"_"随访流水号"_"孕册编号"_PK PRIMARY KEY ("医疗机构代码",
 "随访流水号",
 "孕册编号")
);
COMMENT ON COLUMN "(孕产妇)产后访视记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."下次随访日期" IS '对孕产妇进行下次随访的公元纪年日期';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."转入机构科室名称" IS '转入科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."转入医疗机构名称" IS '转入医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."转诊原因" IS '对孕产妇转诊原因的简要描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."转诊标志" IS '标识孕产妇是否转诊的标志';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."健康保健指导其他" IS '除上述健康保健指导外，其他孕产妇健康指导类别名称';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."健康保健指导名称" IS '对孕产妇进行健康指导类别在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."健康保健指导代码" IS '对孕产妇进行健康指导类别在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."健康评估异常结果描述" IS '孕产妇健康评估异常结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."健康评估异常标志" IS '标识孕产妇健康评估结论是否异常的标志';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."其他检查结果" IS '除上述内容外，其他检查结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."伤口愈合状况其他" IS '除上述类别外，其他伤口愈合状况的名称';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."伤口愈合状况名称" IS '伤口愈合状况所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."伤口愈合状况代码" IS '伤口愈合状况所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."子宫异常描述" IS '子宫检查结果异常情况描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."子宫异常标志" IS '标识子宫检查结果是否异常';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."恶露状况" IS '对产妇产后恶露检查结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."恶露异常标志" IS '标识恶露检查是否存在异常';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."右侧乳腺检查结果其他" IS '除上述类别外，其他右侧乳腺检查结果的名称';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."右侧乳腺检查结果名称" IS '受检者右侧乳腺检查结果的类别在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."右侧乳腺检查结果代码" IS '受检者右侧乳腺检查结果的类别在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."左侧乳腺检查结果其他" IS '除上述类别外，其他左侧乳腺检查结果的名称';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."左侧乳腺检查结果名称" IS '受检者左侧乳腺检查结果的类别在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."左侧乳腺检查结果代码" IS '受检者左侧乳腺检查结果的类别在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."乳房异常描述" IS '乳房检查结果异常情况描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."乳房异常标志" IS '标识乳房检查结果是否异常';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."心理状况描述" IS '产妇心理状况的详细描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."健康状况描述" IS '产妇健康状况的详细描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."产后天数" IS '产妇产后检查日期距分娩日期的累计天数,计量单位为d';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."出院日期" IS '完成出院手续办理时的公元纪年日期';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."分娩日期" IS '孕妇完成分娩时的公元纪年日期';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."随访机构名称" IS '随访机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."随访机构代码" IS '随访机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."随访医生姓名" IS '随访医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."随访医生工号" IS '随访医师的工号';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."随访日期" IS '对患者进行随访时当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."孕册编号" IS '按照某一特定编码规则赋予孕产妇孕册的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."随访流水号" IS '按照一定编码规则赋予产后访视记录的顺序号';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后访视记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON TABLE "(孕产妇)产后访视记录" IS '产妇出院一周内医务人员到产妇家中进行产后检查的结果信息';


CREATE TABLE IF NOT EXISTS "(
孕产妇)产后42天检查" ("随访医生姓名" varchar (50) DEFAULT NULL,
 "随访医生工号" varchar (20) DEFAULT NULL,
 "随访日期" date DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "孕册编号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "下次随访日期" date DEFAULT NULL,
 "转入机构科室名称" varchar (100) DEFAULT NULL,
 "转入医疗机构名称" varchar (70) DEFAULT NULL,
 "转诊原因" varchar (100) DEFAULT NULL,
 "转诊标志" varchar (1) DEFAULT NULL,
 "结案标志" varchar (1) DEFAULT NULL,
 "健康保健指导其他" varchar (100) DEFAULT NULL,
 "健康保健指导名称" varchar (100) DEFAULT NULL,
 "健康保健指导代码" varchar (30) DEFAULT NULL,
 "产妇恢复异常结果描述" varchar (100) DEFAULT NULL,
 "产妇恢复标志" varchar (1) DEFAULT NULL,
 "其他检查结果" varchar (100) DEFAULT NULL,
 "伤口愈合状况其他" varchar (100) DEFAULT NULL,
 "伤口愈合状况名称" varchar (50) DEFAULT NULL,
 "伤口愈合状况代码" varchar (2) DEFAULT NULL,
 "子宫异常描述" varchar (100) DEFAULT NULL,
 "子宫异常标志" varchar (1) DEFAULT NULL,
 "恶露状况" varchar (100) DEFAULT NULL,
 "恶露异常标志" varchar (1) DEFAULT NULL,
 "右侧乳腺检查结果其他" varchar (100) DEFAULT NULL,
 "右侧乳腺检查结果名称" varchar (100) DEFAULT NULL,
 "右侧乳腺检查结果代码" varchar (30) DEFAULT NULL,
 "左侧乳腺检查结果其他" varchar (100) DEFAULT NULL,
 "左侧乳腺检查结果名称" varchar (100) DEFAULT NULL,
 "左侧乳腺检查结果代码" varchar (30) DEFAULT NULL,
 "乳房异常描述" varchar (100) DEFAULT NULL,
 "乳房异常标志" varchar (1) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "心理状况描述" varchar (100) DEFAULT NULL,
 "健康状况描述" varchar (100) DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "产后天数" decimal (2,
 0) DEFAULT NULL,
 "出院日期" date DEFAULT NULL,
 "分娩日期" date DEFAULT NULL,
 "随访机构名称" varchar (70) DEFAULT NULL,
 "随访机构代码" varchar (22) DEFAULT NULL,
 CONSTRAINT "(孕产妇)产后42天检查"_"孕册编号"_"医疗机构代码"_PK PRIMARY KEY ("孕册编号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(孕产妇)产后42天检查"."随访机构代码" IS '随访机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."随访机构名称" IS '随访机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."分娩日期" IS '孕妇完成分娩时的公元纪年日期';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."出院日期" IS '完成出院手续办理时的公元纪年日期';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."产后天数" IS '产妇产后检查日期距分娩日期的累计天数,计量单位为d';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."健康状况描述" IS '产妇健康状况的详细描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."心理状况描述" IS '产妇心理状况的详细描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."乳房异常标志" IS '标识乳房检查结果是否异常';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."乳房异常描述" IS '乳房检查结果异常情况描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."左侧乳腺检查结果代码" IS '受检者左侧乳腺检查结果的类别在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."左侧乳腺检查结果名称" IS '受检者左侧乳腺检查结果的类别在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."左侧乳腺检查结果其他" IS '除上述类别外，其他左侧乳腺检查结果的名称';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."右侧乳腺检查结果代码" IS '受检者右侧乳腺检查结果的类别在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."右侧乳腺检查结果名称" IS '受检者右侧乳腺检查结果的类别在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."右侧乳腺检查结果其他" IS '除上述类别外，其他右侧乳腺检查结果的名称';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."恶露异常标志" IS '标识恶露检查是否存在异常';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."恶露状况" IS '对产妇产后恶露检查结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."子宫异常标志" IS '标识子宫检查结果是否异常';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."子宫异常描述" IS '子宫检查结果异常情况描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."伤口愈合状况代码" IS '伤口愈合状况所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."伤口愈合状况名称" IS '伤口愈合状况所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."伤口愈合状况其他" IS '除上述类别外，其他伤口愈合状况的名称';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."其他检查结果" IS '除上述内容外，其他检查结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."产妇恢复标志" IS '标识产妇是否恢复的标志';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."产妇恢复异常结果描述" IS '产妇恢复异常结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."健康保健指导代码" IS '对孕产妇进行健康指导类别在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."健康保健指导名称" IS '对孕产妇进行健康指导类别在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."健康保健指导其他" IS '除上述健康保健指导外，其他孕产妇健康指导类别名称';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."结案标志" IS '标识孕册是否结案的标志';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."转诊标志" IS '标识孕产妇是否转诊的标志';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."转诊原因" IS '对孕产妇转诊原因的简要描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."转入医疗机构名称" IS '转入医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."转入机构科室名称" IS '转入科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."下次随访日期" IS '对孕产妇进行下次随访的公元纪年日期';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."孕册编号" IS '按照某一特定编码规则赋予孕产妇孕册的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."随访日期" IS '对患者进行随访时当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."随访医生工号" IS '随访医师的工号';
COMMENT ON COLUMN "(孕产妇)产后42天检查"."随访医生姓名" IS '随访医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON TABLE "(孕产妇)产后42天检查" IS '产后42天产妇医院进行检查的结果信息';


CREATE TABLE IF NOT EXISTS "(
冠心病)患者用药记录" ("数据更新时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "药物使用途径代码" varchar (32) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "用药流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "专项档案标识号" varchar (64) DEFAULT NULL,
 "随访流水号" varchar (64) DEFAULT NULL,
 "中药类别代码" varchar (2) DEFAULT NULL,
 "中药类别名称" varchar (50) DEFAULT NULL,
 "药物名称" varchar (50) DEFAULT NULL,
 "药物使用频率名称" varchar (20) DEFAULT NULL,
 "药物使用剂量单位" varchar (6) DEFAULT NULL,
 "药物使用次剂量" decimal (4,
 2) DEFAULT NULL,
 "药物使用总剂量" decimal (11,
 2) DEFAULT NULL,
 "药物使用途径名称" varchar (50) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 CONSTRAINT "(冠心病)患者用药记录"_"医疗机构代码"_"用药流水号"_PK PRIMARY KEY ("医疗机构代码",
 "用药流水号")
);
COMMENT ON COLUMN "(冠心病)患者用药记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(冠心病)患者用药记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(冠心病)患者用药记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(冠心病)患者用药记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(冠心病)患者用药记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(冠心病)患者用药记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(冠心病)患者用药记录"."药物使用途径名称" IS '药物的给药途径在特定编码体系中的名称，如口服、静滴、喷喉等';
COMMENT ON COLUMN "(冠心病)患者用药记录"."药物使用总剂量" IS '在一定时间段内使用药物的总量。根据单次剂量、频次计算得到的总量';
COMMENT ON COLUMN "(冠心病)患者用药记录"."药物使用次剂量" IS '单次使用药物的剂量，按剂量单位计';
COMMENT ON COLUMN "(冠心病)患者用药记录"."药物使用剂量单位" IS '药物使用剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "(冠心病)患者用药记录"."药物使用频率名称" IS '单位时间内药物使用频次类别的标准名称，如每天两次、每周两次、睡前一次等';
COMMENT ON COLUMN "(冠心病)患者用药记录"."药物名称" IS '药品在特定编码体系中的名称。对于医疗机构制剂，此处填写医疗机构制剂名称';
COMMENT ON COLUMN "(冠心病)患者用药记录"."中药类别名称" IS '中药使用类别的标准名称，如未使用、中成药、中草药、其他中药等';
COMMENT ON COLUMN "(冠心病)患者用药记录"."中药类别代码" IS '中药使用类别(如未使用、中成药、中草药、其他中药)在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)患者用药记录"."随访流水号" IS '按照一定编码规则赋予评估记录的顺序号';
COMMENT ON COLUMN "(冠心病)患者用药记录"."专项档案标识号" IS '按照一定编码规则赋予个人的唯一标识。与【(冠心病)专项档案JB_GXB_ZXDA】表中的【专项档案标识ZXDAID】字段关联';
COMMENT ON COLUMN "(冠心病)患者用药记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(冠心病)患者用药记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(冠心病)患者用药记录"."用药流水号" IS '按照一定编码规则赋予产后访视记录的顺序号';
COMMENT ON COLUMN "(冠心病)患者用药记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(冠心病)患者用药记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(冠心病)患者用药记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(冠心病)患者用药记录"."药物使用途径代码" IS '药物使用途径(如口服、静滴、喷喉等)在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)患者用药记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(冠心病)患者用药记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(冠心病)患者用药记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(冠心病)患者用药记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(冠心病)患者用药记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(冠心病)患者用药记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON TABLE "(冠心病)患者用药记录" IS '冠心病患者用药记录，包括药物类别、名称、使用频率、剂量、途径等';


CREATE TABLE IF NOT EXISTS "(
冠心病)专项档案" ("确诊日期" date DEFAULT NULL,
 "确诊医院名称" varchar (200) DEFAULT NULL,
 "首次发病标志" varchar (1) DEFAULT NULL,
 "终止管理标志" varchar (1) DEFAULT NULL,
 "终止日期" date DEFAULT NULL,
 "终止理由代码" varchar (2) DEFAULT NULL,
 "终止理由名称" varchar (50) DEFAULT NULL,
 "终止理由其他" varchar (100) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "确诊医院类型" varchar (10) DEFAULT NULL,
 "建卡日期" date DEFAULT NULL,
 "报告卡标识号" varchar (50) DEFAULT NULL,
 "居住地-邮政编码" varchar (6) DEFAULT NULL,
 "居住地-门牌号码" varchar (70) DEFAULT NULL,
 "居住地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "居住地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "居住地-居委名称" varchar (32) DEFAULT NULL,
 "居住地-居委代码" varchar (12) DEFAULT NULL,
 "居住地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "居住地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "居住地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "居住地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "居住地-市(地区、州)名称" varchar (32) DEFAULT NULL,
 "居住地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "居住地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "居住地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "居住地-行政区划代码" varchar (12) DEFAULT NULL,
 "居住地-详细地址" varchar (200) DEFAULT NULL,
 "户籍地-邮政编码" varchar (6) DEFAULT NULL,
 "户籍地-门牌号码" varchar (100) DEFAULT NULL,
 "户籍地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "户籍地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "户籍地-居委名称" varchar (32) DEFAULT NULL,
 "户籍地-居委代码" varchar (12) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "户籍地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "户籍地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "户籍地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "户籍地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "户籍地-行政区划代码" varchar (12) DEFAULT NULL,
 "户籍地-详细地址" varchar (200) DEFAULT NULL,
 "工作单位名称" varchar (70) DEFAULT NULL,
 "联系电话号码" varchar (20) DEFAULT NULL,
 "手机号码" varchar (20) DEFAULT NULL,
 "医疗费用支付方式其他" varchar (100) DEFAULT NULL,
 "医疗费用支付方式名称" varchar (100) DEFAULT NULL,
 "医疗费用支付方式代码" varchar (30) DEFAULT NULL,
 "婚姻状况名称" varchar (50) DEFAULT NULL,
 "婚姻状况代码" varchar (2) DEFAULT NULL,
 "学历名称" varchar (50) DEFAULT NULL,
 "学历代码" varchar (5) DEFAULT NULL,
 "职业类别名称" varchar (60) DEFAULT NULL,
 "职业类别代码" varchar (4) DEFAULT NULL,
 "民族名称" varchar (50) DEFAULT NULL,
 "民族代码" varchar (2) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "专项档案标识号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "建卡机构代码" varchar (22) DEFAULT NULL,
 "建卡机构名称" varchar (70) DEFAULT NULL,
 "建卡医生工号" varchar (20) DEFAULT NULL,
 "建卡医生姓名" varchar (50) DEFAULT NULL,
 "危险因素描述" varchar (255) DEFAULT NULL,
 "身体活动受限情况" varchar (255) DEFAULT NULL,
 "门(急)诊号" varchar (32) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "实足年龄" decimal (6,
 0) DEFAULT NULL,
 "ICD诊断代码" varchar (20) DEFAULT NULL,
 "疾病类型" varchar (10) DEFAULT NULL,
 "病史" varchar (200) DEFAULT NULL,
 "病史摘要" varchar (500) DEFAULT NULL,
 "发病日期" date DEFAULT NULL,
 CONSTRAINT "(冠心病)专项档案"_"专项档案标识号"_"医疗机构代码"_PK PRIMARY KEY ("专项档案标识号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(冠心病)专项档案"."发病日期" IS '疾病发病时的公元纪年日期';
COMMENT ON COLUMN "(冠心病)专项档案"."病史摘要" IS '对患者病情摘要的详细描述';
COMMENT ON COLUMN "(冠心病)专项档案"."病史" IS '对个体既往健康状况和疾病(含外伤)的详细描述';
COMMENT ON COLUMN "(冠心病)专项档案"."疾病类型" IS '疾病类型的描述';
COMMENT ON COLUMN "(冠心病)专项档案"."ICD诊断代码" IS '平台中心ICD诊断代码';
COMMENT ON COLUMN "(冠心病)专项档案"."实足年龄" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "(冠心病)专项档案"."住院号" IS '按照某一特定编码规则赋予住院对象的顺序号';
COMMENT ON COLUMN "(冠心病)专项档案"."门(急)诊号" IS '按照某一特定编码规则赋予门(急)诊就诊对象的顺序号';
COMMENT ON COLUMN "(冠心病)专项档案"."身体活动受限情况" IS '身体活动受限情况的描述';
COMMENT ON COLUMN "(冠心病)专项档案"."危险因素描述" IS '危险因素的详细描述';
COMMENT ON COLUMN "(冠心病)专项档案"."建卡医生姓名" IS '建卡医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(冠心病)专项档案"."建卡医生工号" IS '建卡医师的工号';
COMMENT ON COLUMN "(冠心病)专项档案"."建卡机构名称" IS '建卡机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(冠心病)专项档案"."建卡机构代码" IS '按照某一特定编码规则赋予建卡机构的唯一标识';
COMMENT ON COLUMN "(冠心病)专项档案"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(冠心病)专项档案"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(冠心病)专项档案"."专项档案标识号" IS '按照某一特定规则赋予专项档案的唯一标识';
COMMENT ON COLUMN "(冠心病)专项档案"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(冠心病)专项档案"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(冠心病)专项档案"."姓名" IS '本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(冠心病)专项档案"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "(冠心病)专项档案"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "(冠心病)专项档案"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(冠心病)专项档案"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "(冠心病)专项档案"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."民族名称" IS '所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "(冠心病)专项档案"."职业类别代码" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."职业类别名称" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的名称';
COMMENT ON COLUMN "(冠心病)专项档案"."学历代码" IS '个体受教育最高程度的类别(如研究生教育、大学本科、专科教育等)在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."学历名称" IS '个体受教育最高程度的类别标准名称，如研究生教育、大学本科、专科教育等';
COMMENT ON COLUMN "(冠心病)专项档案"."婚姻状况代码" IS '当前婚姻状况(已婚、未婚、初婚等)在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."婚姻状况名称" IS '当前婚姻状况的标准名称，如已婚、未婚、初婚等';
COMMENT ON COLUMN "(冠心病)专项档案"."医疗费用支付方式代码" IS '患者医疗费用支付方式类别(如城镇职工基本医疗保险、城镇居民基本医疗保险等)在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."医疗费用支付方式名称" IS '患者医疗费用支付方式类别在特定编码体系中的名称，如城镇职工基本医疗保险、城镇居民基本医疗保险等';
COMMENT ON COLUMN "(冠心病)专项档案"."医疗费用支付方式其他" IS '除上述医疗费用支付方式外，其他支付方式的名称';
COMMENT ON COLUMN "(冠心病)专项档案"."手机号码" IS '居民本人的手机号码';
COMMENT ON COLUMN "(冠心病)专项档案"."联系电话号码" IS '居民本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(冠心病)专项档案"."工作单位名称" IS '个体工作单位的组织机构名称';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-详细地址" IS '户籍地址的详细描述';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-行政区划代码" IS '户籍地址所在区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-省(自治区、直辖市)代码" IS '户籍登记所在地址的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-省(自治区、直辖市)名称" IS '户籍登记所在地址的省、自治区或直辖市名称';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-市(地区、州)代码" IS '户籍登记所在地址的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-市(地区、州)名称" IS '户籍登记所在地址的市、地区或州的名称';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-县(市、区)代码" IS '户籍登记所在地址的县(区)的在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-县(市、区)名称" IS '户籍登记所在地址的县(市、区)的名称';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-乡(镇、街道办事处)代码" IS '户籍登记所在地址的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-乡(镇、街道办事处)名称" IS '户籍登记所在地址的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-居委代码" IS '户籍登记所在地址的居民委员会在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-居委名称" IS '户籍登记所在地址的居民委员会名称';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-村(街、路、弄等)代码" IS '户籍登记所在地址的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-村(街、路、弄等)名称" IS '户籍登记所在地址的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-门牌号码" IS '户籍登记所在地址的门牌号码';
COMMENT ON COLUMN "(冠心病)专项档案"."户籍地-邮政编码" IS '户籍地址所在行政区的邮政编码';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-详细地址" IS '现住地址的详细描述';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-行政区划代码" IS '居住地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-省(自治区、直辖市)代码" IS '现住地址中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-省(自治区、直辖市)名称" IS '现住地址中的省、自治区或直辖市名称';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-市(地区、州)代码" IS '现住地址中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-市(地区、州)名称" IS '现住地址中的市、地区或州的名称';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-县(市、区)代码" IS '现住地址中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-县(市、区)名称" IS '现住地址中的县或区名称';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-乡(镇、街道办事处)代码" IS '本人现住地址中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-乡(镇、街道办事处)名称" IS '本人现住地址中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-居委代码" IS '现住地址所属的居民委员会在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-居委名称" IS '现住地址所属的居民委员会名称';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-村(街、路、弄等)代码" IS '本人现住地址中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-村(街、路、弄等)名称" IS '本人现住地址中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-门牌号码" IS '本人现住地址中的门牌号码';
COMMENT ON COLUMN "(冠心病)专项档案"."居住地-邮政编码" IS '现住地址中所在行政区的邮政编码';
COMMENT ON COLUMN "(冠心病)专项档案"."报告卡标识号" IS '按照某一特定规则赋予报告卡的唯一标识';
COMMENT ON COLUMN "(冠心病)专项档案"."建卡日期" IS '完成建卡时的公元纪年日期';
COMMENT ON COLUMN "(冠心病)专项档案"."确诊医院类型" IS '确诊医院的机构类型';
COMMENT ON COLUMN "(冠心病)专项档案"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(冠心病)专项档案"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(冠心病)专项档案"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(冠心病)专项档案"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(冠心病)专项档案"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(冠心病)专项档案"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(冠心病)专项档案"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(冠心病)专项档案"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(冠心病)专项档案"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(冠心病)专项档案"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(冠心病)专项档案"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(冠心病)专项档案"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(冠心病)专项档案"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(冠心病)专项档案"."终止理由其他" IS '患者终止管理的其他原因描述';
COMMENT ON COLUMN "(冠心病)专项档案"."终止理由名称" IS '患者终止管理的原因类别在特定编码体系中的名称，如死亡、迁出、失访等';
COMMENT ON COLUMN "(冠心病)专项档案"."终止理由代码" IS '患者终止管理的原因类别在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)专项档案"."终止日期" IS '高血压患者终止管理当日的公元纪年日期';
COMMENT ON COLUMN "(冠心病)专项档案"."终止管理标志" IS '标识该高血压患者是否终止管理';
COMMENT ON COLUMN "(冠心病)专项档案"."首次发病标志" IS '标识患者是否首次发烧的标志';
COMMENT ON COLUMN "(冠心病)专项档案"."确诊医院名称" IS '确诊医院的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(冠心病)专项档案"."确诊日期" IS '糖尿病确诊的公元纪年日期';
COMMENT ON TABLE "(冠心病)专项档案" IS '冠心病患者基本信息，包括人口统计学信息、疾病危险因素、确诊时间等';


CREATE TABLE IF NOT EXISTS "(
儿童健康体检)基本信息" ("医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "药物过敏史" varchar (200) DEFAULT NULL,
 "患者与本人关系名称" varchar (50) DEFAULT NULL,
 "患者与本人关系代码" varchar (2) NOT NULL,
 "与遗传有关的家族史" varchar (100) DEFAULT NULL,
 "Rh血型名称" varchar (50) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "ABO血型名称" varchar (50) DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "会爬(月)" decimal (2,
 0) DEFAULT NULL,
 "独坐(月)" decimal (2,
 0) DEFAULT NULL,
 "翻身(月)" decimal (2,
 0) DEFAULT NULL,
 "抬头(月)" decimal (2,
 0) DEFAULT NULL,
 "新生儿疾病筛查结果" varchar (100) DEFAULT NULL,
 "新生儿筛查项目其他" varchar (100) DEFAULT NULL,
 "新生儿筛查项目名称" varchar (100) DEFAULT NULL,
 "新生儿筛查项目代码" varchar (30) DEFAULT NULL,
 "新生儿听力筛查结果名称" varchar (50) DEFAULT NULL,
 "新生儿听力筛查结果代码" varchar (2) DEFAULT NULL,
 "新生儿听力筛查情况名称" varchar (50) DEFAULT NULL,
 "新生儿听力筛查情况代码" varchar (2) DEFAULT NULL,
 "新生儿畸型描述" varchar (100) DEFAULT NULL,
 "新生儿畸型标志" varchar (1) DEFAULT NULL,
 "Apgar评分10" decimal (2,
 0) DEFAULT NULL,
 "Apgar评分5" decimal (2,
 0) DEFAULT NULL,
 "Apgar评分1" decimal (2,
 0) DEFAULT NULL,
 "新生儿窒息标志" varchar (1) DEFAULT NULL,
 "出生胸围(cm)" decimal (4,
 1) DEFAULT NULL,
 "出生头围(cm)" decimal (4,
 1) DEFAULT NULL,
 "出生身长(cm)" decimal (4,
 1) DEFAULT NULL,
 "出生体重(g)" decimal (4,
 0) DEFAULT NULL,
 "出生孕周(天)" decimal (1,
 0) DEFAULT NULL,
 "出生孕周(周)" decimal (2,
 0) DEFAULT NULL,
 "出生顺序" decimal (1,
 0) DEFAULT NULL,
 "双多胎标志" varchar (1) DEFAULT NULL,
 "分娩方式其他" varchar (100) DEFAULT NULL,
 "分娩方式名称" varchar (50) DEFAULT NULL,
 "分娩方式代码" varchar (2) DEFAULT NULL,
 "分娩机构名称" varchar (70) DEFAULT NULL,
 "分娩机构代码" varchar (22) DEFAULT NULL,
 "母亲妊娠合并症描述" varchar (100) DEFAULT NULL,
 "居住地-邮政编码" varchar (6) DEFAULT NULL,
 "居住地-门牌号码" varchar (70) DEFAULT NULL,
 "居住地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "居住地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "居住地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "居住地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "居住地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "居住地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "居住地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "居住地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "居住地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "居住地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "居住地-行政区划代码" varchar (12) DEFAULT NULL,
 "居住地-详细地址" varchar (200) DEFAULT NULL,
 "户籍地-邮政编码" varchar (6) DEFAULT NULL,
 "户籍地-门牌号码" varchar (100) DEFAULT NULL,
 "户籍地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "户籍地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "户籍地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "户籍地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "户籍地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "户籍地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "户籍地-行政区划代码" varchar (12) DEFAULT NULL,
 "户籍地-详细地址" varchar (200) DEFAULT NULL,
 "母亲工作单位名称" varchar (70) DEFAULT NULL,
 "母亲电话号码" varchar (20) DEFAULT NULL,
 "母亲职业名称" varchar (100) DEFAULT NULL,
 "母亲职业代码" varchar (20) DEFAULT NULL,
 "母亲出生日期" date DEFAULT NULL,
 "母亲姓名" varchar (50) DEFAULT NULL,
 "父亲工作单位名称" varchar (70) DEFAULT NULL,
 "父亲电话号码" varchar (20) DEFAULT NULL,
 "父亲职业名称" varchar (100) DEFAULT NULL,
 "父亲职业代码" varchar (20) DEFAULT NULL,
 "父亲出生日期" date DEFAULT NULL,
 "父亲姓名" varchar (50) DEFAULT NULL,
 "表单编号" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "业务管理机构名称" varchar (70) DEFAULT NULL,
 "业务管理机构代码" varchar (22) DEFAULT NULL,
 "建册年龄(天)" decimal (2,
 0) DEFAULT NULL,
 "建册年龄(月)" decimal (2,
 0) DEFAULT NULL,
 "建册医生姓名" varchar (50) DEFAULT NULL,
 "建册医生工号" varchar (20) DEFAULT NULL,
 "建册机构名称" varchar (70) DEFAULT NULL,
 "建册机构代码" varchar (22) DEFAULT NULL,
 "建册时间" timestamp DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 CONSTRAINT "(儿童健康体检)基本信息"_"医疗机构代码"_"患者与本人关系代码"_"个人唯一标识号"_PK PRIMARY KEY ("医疗机构代码",
 "患者与本人关系代码",
 "个人唯一标识号")
);
COMMENT ON COLUMN "(儿童健康体检)基本信息"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."建册时间" IS '儿童体检基本信息初次建册完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."建册机构代码" IS '儿童建册医疗机构的组织机构代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."建册机构名称" IS '儿童建册医疗机构的组织机构名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."建册医生工号" IS '建册医师的工号';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."建册医生姓名" IS '建册医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."建册年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."建册年龄(天)" IS '年龄不足1个月的实足年龄的天数';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."业务管理机构代码" IS '儿童体检业务管理机构的组织机构代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."业务管理机构名称" IS '儿童体检管理机构的组织机构名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."姓名" IS '儿童在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."证件类型代码" IS '儿童身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."证件类型名称" IS '儿童身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."出生日期" IS '儿童出生当日的公元纪年日期';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."表单编号" IS '按照某一特定规则赋予儿童体检表单的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."父亲姓名" IS '父亲在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."父亲出生日期" IS '儿童父亲出生当日的公元纪年日期';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."父亲职业代码" IS '父亲从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."父亲职业名称" IS '父亲从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."父亲电话号码" IS '父亲联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."父亲工作单位名称" IS '父亲工作单位或学校的组织机构名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."母亲姓名" IS '母亲在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."母亲出生日期" IS '儿童母亲出生当日的公元纪年日期';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."母亲职业代码" IS '母亲从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."母亲职业名称" IS '母亲从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."母亲电话号码" IS '母亲联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."母亲工作单位名称" IS '母亲工作单位或学校的组织机构名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-详细地址" IS '儿童户籍地址的详细描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-行政区划代码" IS '儿童户籍地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-省(自治区、直辖市)代码" IS '儿童户籍地中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-省(自治区、直辖市)名称" IS '儿童户籍地中的省、自治区或直辖市名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-市(地区、州)代码" IS '儿童户籍地中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-市(地区、州)名称" IS '儿童户籍地中的市、地区或州的名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-县(市、区)代码" IS '儿童户籍地中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-县(市、区)名称" IS '儿童户籍地中的县或区名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-乡(镇、街道办事处)代码" IS '儿童户籍地中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-乡(镇、街道办事处)名称" IS '儿童户籍地中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-村(街、路、弄等)代码" IS '儿童户籍地中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-村(街、路、弄等)名称" IS '儿童户籍地中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-门牌号码" IS '儿童户籍地中的门牌号码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."户籍地-邮政编码" IS '户籍地址地址的邮政编码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-详细地址" IS '儿童居住地址的详细描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-行政区划代码" IS '儿童居住地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-省(自治区、直辖市)代码" IS '儿童居住地中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-省(自治区、直辖市)名称" IS '儿童居住地中的省、自治区或直辖市名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-市(地区、州)代码" IS '儿童居住地中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-市(地区、州)名称" IS '儿童居住地中的市、地区或州的名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-县(市、区)代码" IS '儿童居住地中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-县(市、区)名称" IS '儿童居住地中的县或区名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-乡(镇、街道办事处)代码" IS '儿童居住地中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-乡(镇、街道办事处)名称" IS '儿童居住地中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-村(街、路、弄等)代码" IS '儿童居住地中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-村(街、路、弄等)名称" IS '儿童居住地中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-门牌号码" IS '儿童居住地中的门牌号码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."居住地-邮政编码" IS '居住地址的邮政编码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."母亲妊娠合并症描述" IS '母亲妊娠合并症的详细描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."分娩机构代码" IS '分娩机构的组织机构代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."分娩机构名称" IS '分娩管理机构的组织机构名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."分娩方式代码" IS '儿童母亲分娩方式在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."分娩方式名称" IS '儿童母亲分娩方式在特定编码体系中的名称，如剖宫产，阴道分娩等';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."分娩方式其他" IS '儿童母亲其他分娩方式的详细描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."双多胎标志" IS '标识该儿童是否为双胞胎或多胞胎';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."出生顺序" IS '该儿童的出生顺序，如1,2 ，3等';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."出生孕周(周)" IS '儿童出生时母亲妊娠时长的周数，计量单位为周';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."出生孕周(天)" IS '儿童出生时母亲妊娠时长的天数，计量单位为天';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."出生体重(g)" IS '儿童出生后1h内体重的测量值，计量单位为g';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."出生身长(cm)" IS '儿童出生后1h内身长的测量值，计量单位为cm';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."出生头围(cm)" IS '儿童出生时头部周长的测量值，计量单位为cm';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."出生胸围(cm)" IS '儿童出生时胸部周长的测量值，计量单位为cm';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."新生儿窒息标志" IS '标识儿童出生时是否窒息';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."Apgar评分1" IS '1分钟时对个体的呼吸、心率、皮肤颜色、肌张力及对刺激的反应等五项指标的评分结果值，计量单位为分';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."Apgar评分5" IS '5分钟时对个体的呼吸、心率、皮肤颜色、肌张力及对刺激的反应等五项指标的评分结果值，计量单位为分';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."Apgar评分10" IS '10分钟时对个体的呼吸、心率、皮肤颜色、肌张力及对刺激的反应等五项指标的评分结果值，计量单位为分';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."新生儿畸型标志" IS '标识新生儿出生时是否畸形';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."新生儿畸型描述" IS '新生儿畸形情况描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."新生儿听力筛查情况代码" IS '新生儿听力筛查情况在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."新生儿听力筛查情况名称" IS '新生儿听力筛查情况在特定编码体系中的名称，如已筛查、未筛查、不详等';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."新生儿听力筛查结果代码" IS '新生儿听力筛查的结果在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."新生儿听力筛查结果名称" IS '新生儿听力筛查的结果在特定编码体系中的名称，如通过、未通过等';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."新生儿筛查项目代码" IS '新生儿筛查项目在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."新生儿筛查项目名称" IS '新生儿筛查项目在特定编码体系中的名称，如先天性甲状腺功能减低症、地中海贫血等';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."新生儿筛查项目其他" IS '新生儿其他筛查项目描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."新生儿疾病筛查结果" IS '筛查结果的结论性的详细描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."抬头(月)" IS '婴幼儿独自抬头时的月龄，计量单位为月';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."翻身(月)" IS '婴幼儿翻身时的月龄，计量单位为月';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."独坐(月)" IS '婴幼儿独自坐立时的月龄，计量单位为月';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."会爬(月)" IS '婴幼儿独自爬行时的月龄，计量单位为月';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."ABO血型代码" IS '受检者按照ABO血型系统决定的血型在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."ABO血型名称" IS '受检者按照ABO血型系统决定的血型的标准名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."Rh血型代码" IS '进行血型检查明确，或既往病历资料能够明确的患者Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."Rh血型名称" IS '进行血型检查明确，或既往病历资料能够明确的患者Rh血型的类别在特定编码体系中的名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."与遗传有关的家族史" IS '与遗产相关的家族史描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."患者与本人关系代码" IS '遗传病患者与儿童本人的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."患者与本人关系名称" IS '遗传病患者与儿童本人关系类别在特定编码体系中的名称，如户主、配偶、子女、父母等';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."药物过敏史" IS '儿童药物过敏史描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)基本信息"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON TABLE "(儿童健康体检)基本信息" IS '参加健康体检的儿童基本信息，包括姓名、性别、证件号码、父母信息、出生情况等';


CREATE TABLE IF NOT EXISTS "(
健康体检)职业暴露危险因素" ("危险因素名称" varchar (50) DEFAULT NULL,
 "危险因素种类名称" varchar (50) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "危险因素种类代码" varchar (2) NOT NULL,
 "体检流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "职业防护措施标志" varchar (1) DEFAULT NULL,
 CONSTRAINT "(健康体检)职业暴露危险因素"_"危险因素种类代码"_"体检流水号"_"医疗机构代码"_PK PRIMARY KEY ("危险因素种类代码",
 "体检流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."职业防护措施标志" IS '标识受检者是否有针对职业危害因素的预防保护的具体措施';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."登记机构代码" IS '按照某一特定编码规则赋予登记医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."修改时间" IS '修改当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."修改机构代码" IS '按照某一特定编码规则赋予修改医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."体检流水号" IS '按照某一特定编码规则赋予体检就诊记录的顺序号';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."危险因素种类代码" IS '职业病危害因素的类别在特定编码体系中的代码';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."危险因素种类名称" IS '职业病危害因素的类别在特定编码体系中的名称，如粉尘、化学有害因素等';
COMMENT ON COLUMN "(健康体检)职业暴露危险因素"."危险因素名称" IS '危险因素名称的详细描述';
COMMENT ON TABLE "(健康体检)职业暴露危险因素" IS '体检职业暴露危险因素子表，包括危险因素种类、危险因素名称、职业防护措施等';


CREATE TABLE IF NOT EXISTS "(
健康体检)现存主要健康问题" ("数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "体检流水号" varchar (64) NOT NULL,
 "问题流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "现存主要健康问题代码" varchar (20) DEFAULT NULL,
 "现存主要健康问题名称" varchar (100) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 CONSTRAINT "(健康体检)现存主要健康问题"_"医疗机构代码"_"体检流水号"_"问题流水号"_PK PRIMARY KEY ("医疗机构代码",
 "体检流水号",
 "问题流水号")
);
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."修改机构代码" IS '按照某一特定编码规则赋予修改医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."修改时间" IS '修改当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."登记机构代码" IS '按照某一特定编码规则赋予登记医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."现存主要健康问题名称" IS '曾经出现或一直存在,并影响目前身体健康状况的疾病在特定编码体系中的名称';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."现存主要健康问题代码" IS '曾经出现或一直存在,并影响目前身体健康状况的疾病在特定编码体系中的代码';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."问题流水号" IS '按照某一特性编码规则赋予家庭成员信息记录的顺序号';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."体检流水号" IS '按照一定编码规则赋予问题记录的顺序号';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(健康体检)现存主要健康问题"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON TABLE "(健康体检)现存主要健康问题" IS '体检现存主要健康问题子表，包括问题名称、登记时间等';


CREATE TABLE IF NOT EXISTS "(
健康体检)家庭病床史" ("医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "建床病案号" varchar (18) DEFAULT NULL,
 "建床机构名称" varchar (70) DEFAULT NULL,
 "建床机构代码" varchar (22) DEFAULT NULL,
 "建立原因" varchar (100) DEFAULT NULL,
 "建立天数" decimal (5,
 0) DEFAULT NULL,
 "撤床时间" timestamp DEFAULT NULL,
 "建床时间" timestamp DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "家床流水号" varchar (64) NOT NULL,
 "体检流水号" varchar (64) NOT NULL,
 CONSTRAINT "(健康体检)家庭病床史"_"医疗机构代码"_"家床流水号"_"体检流水号"_PK PRIMARY KEY ("医疗机构代码",
 "家床流水号",
 "体检流水号")
);
COMMENT ON COLUMN "(健康体检)家庭病床史"."体检流水号" IS '按照某一特定编码规则赋予家庭病床的顺序号';
COMMENT ON COLUMN "(健康体检)家庭病床史"."家床流水号" IS '按照某一特定编码规则赋予体检就诊记录的顺序号';
COMMENT ON COLUMN "(健康体检)家庭病床史"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(健康体检)家庭病床史"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(健康体检)家庭病床史"."建床时间" IS '家庭病床创建当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)家庭病床史"."撤床时间" IS '家庭病床撤销当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)家庭病床史"."建立天数" IS '患者实际建立家庭病床的天数';
COMMENT ON COLUMN "(健康体检)家庭病床史"."建立原因" IS '患者建立家庭病床的原因描述';
COMMENT ON COLUMN "(健康体检)家庭病床史"."建床机构代码" IS '赋予建床机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(健康体检)家庭病床史"."建床机构名称" IS '赋予建床机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)家庭病床史"."建床病案号" IS '按照某一特定编码规则赋予患者建立家庭病床的病案号。原则上，同一患者在同一医疗机构建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "(健康体检)家庭病床史"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)家庭病床史"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(健康体检)家庭病床史"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)家庭病床史"."登记机构代码" IS '按照某一特定编码规则赋予登记医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)家庭病床史"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)家庭病床史"."修改时间" IS '修改当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)家庭病床史"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(健康体检)家庭病床史"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)家庭病床史"."修改机构代码" IS '按照某一特定编码规则赋予修改医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)家庭病床史"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)家庭病床史"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(健康体检)家庭病床史"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)家庭病床史"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)家庭病床史"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(健康体检)家庭病床史"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON TABLE "(健康体检)家庭病床史" IS '体检家庭病床史，包括建床及撤床日期、建立天数、建立原因等';


CREATE TABLE IF NOT EXISTS "(
健康体检)中医体质" ("数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "中医保健指导" varchar (100) DEFAULT NULL,
 "判定结果名称" varchar (50) DEFAULT NULL,
 "判定结果代码" varchar (2) DEFAULT NULL,
 "中医体质分类名称" varchar (50) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "中医体质分类代码" varchar (2) NOT NULL,
 "体检流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "(健康体检)中医体质"_"中医体质分类代码"_"体检流水号"_"医疗机构代码"_PK PRIMARY KEY ("中医体质分类代码",
 "体检流水号",
 "医疗机构代码")
);
COMMENT ON COLUMN "(健康体检)中医体质"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)中医体质"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(健康体检)中医体质"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(健康体检)中医体质"."体检流水号" IS '按照某一特定编码规则赋予体检就诊记录的顺序号';
COMMENT ON COLUMN "(健康体检)中医体质"."中医体质分类代码" IS '中华中医药学会颁布的《_中医体质分类与判定》中规定的中医体质的在特定编码体系中的代码';
COMMENT ON COLUMN "(健康体检)中医体质"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(健康体检)中医体质"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(健康体检)中医体质"."中医体质分类名称" IS '体质类型在特定编码体系中的名称，包含平和质、气虚质、阳虚质、阴虚质、痰湿质、湿热质、血瘀质、气郁质和特禀质';
COMMENT ON COLUMN "(健康体检)中医体质"."判定结果代码" IS '依据中华中医药学会颁布的《_中医体质分类与判定标准》进行测评得到的中医体质分类判定结果在特定编码体系中的代码';
COMMENT ON COLUMN "(健康体检)中医体质"."判定结果名称" IS '中医体质分类判定结果在特定编码体系中的名称，如是，基本是，倾向是';
COMMENT ON COLUMN "(健康体检)中医体质"."中医保健指导" IS '受检者中医保健指导的详细描述';
COMMENT ON COLUMN "(健康体检)中医体质"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)中医体质"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(健康体检)中医体质"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)中医体质"."登记机构代码" IS '按照某一特定编码规则赋予登记医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)中医体质"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)中医体质"."修改时间" IS '修改当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)中医体质"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(健康体检)中医体质"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)中医体质"."修改机构代码" IS '按照某一特定编码规则赋予修改医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)中医体质"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)中医体质"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(健康体检)中医体质"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON TABLE "(健康体检)中医体质" IS '体检中医体质子表，包括体质分类、体质判定结果、中医保健指导等';


CREATE TABLE IF NOT EXISTS "(
个人)输血史" ("登记机构代码" varchar (22) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "输血流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "输血时间" timestamp DEFAULT NULL,
 "输血原因" varchar (100) DEFAULT NULL,
 "输血品种代码" varchar (10) DEFAULT NULL,
 "输血品种名称" varchar (50) DEFAULT NULL,
 "计量单位代码" varchar (2) DEFAULT NULL,
 "计量单位名称" varchar (50) DEFAULT NULL,
 "输血数量" decimal (5,
 0) DEFAULT NULL,
 "输血反应描述" varchar (100) DEFAULT NULL,
 "输血机构代码" varchar (22) DEFAULT NULL,
 "输血机构名称" varchar (70) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 CONSTRAINT "(个人)输血史"_"医疗机构代码"_"输血流水号"_PK PRIMARY KEY ("医疗机构代码",
 "输血流水号")
);
COMMENT ON COLUMN "(个人)输血史"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个人)输血史"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(个人)输血史"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)输血史"."输血机构名称" IS '输血机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(个人)输血史"."输血机构代码" IS '按照某一特定编码规则赋予输血机构的唯一标识';
COMMENT ON COLUMN "(个人)输血史"."输血反应描述" IS '输血反应情况的详细描述';
COMMENT ON COLUMN "(个人)输血史"."输血数量" IS '输入红细胞、血小板、血浆、全血等的数量，剂量单位为mL';
COMMENT ON COLUMN "(个人)输血史"."计量单位名称" IS '输入血液或血液成分的计量单位在特定编码体系中的名称，可包含汉字的字符，如ml,单位，治疗量等';
COMMENT ON COLUMN "(个人)输血史"."计量单位代码" IS '输入血液或血液成分的计量单位在特定编码体系中的代码.1.红细胞单位为“计数” 2.血小板单位为\r\n“单位” 3.血浆单位为“毫升” 4.全血 单位为“毫升”';
COMMENT ON COLUMN "(个人)输血史"."输血品种名称" IS '输入全血或血液成分类别的标准名称，如红细胞、全血、血小板、血浆等';
COMMENT ON COLUMN "(个人)输血史"."输血品种代码" IS '输入全血或血液成分类别(如红细胞、全血、血小板、血浆等)在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)输血史"."输血原因" IS '表示既往输血的原因';
COMMENT ON COLUMN "(个人)输血史"."输血时间" IS '患者开始进行输血时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)输血史"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(个人)输血史"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(个人)输血史"."输血流水号" IS '按照特定编码规则赋予家族性疾病记录的顺序号';
COMMENT ON COLUMN "(个人)输血史"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(个人)输血史"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(个人)输血史"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个人)输血史"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)输血史"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)输血史"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(个人)输血史"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个人)输血史"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(个人)输血史"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个人)输血史"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(个人)输血史"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)输血史"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON TABLE "(个人)输血史" IS '个人输血史，包括输血时间、输血原因、输血品种、输血数量等';


CREATE TABLE IF NOT EXISTS "(
个人)外伤史" ("数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "外伤流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "外伤名称" varchar (100) DEFAULT NULL,
 "发生时间" timestamp DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "(个人)外伤史"_"医疗机构代码"_"外伤流水号"_PK PRIMARY KEY ("医疗机构代码",
 "外伤流水号")
);
COMMENT ON COLUMN "(个人)外伤史"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)外伤史"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个人)外伤史"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(个人)外伤史"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个人)外伤史"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(个人)外伤史"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)外伤史"."发生时间" IS '患者受到外伤的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)外伤史"."外伤名称" IS '个体发生的外伤的具体名称';
COMMENT ON COLUMN "(个人)外伤史"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(个人)外伤史"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(个人)外伤史"."外伤流水号" IS '按照特定编码规则赋予输血记录的顺序号';
COMMENT ON COLUMN "(个人)外伤史"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(个人)外伤史"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(个人)外伤史"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)外伤史"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(个人)外伤史"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个人)外伤史"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(个人)外伤史"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个人)外伤史"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(个人)外伤史"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON TABLE "(个人)外伤史" IS '个人外伤史，包括外伤名称、发生时间等';


CREATE TABLE IF NOT EXISTS "24h内入院死亡记录" (
"民族代码" varchar (2) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "住院死亡记录流水号" varchar (64) NOT NULL,
 "住院就诊流水号" varchar (32) NOT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "主任医师姓名" varchar (50) DEFAULT NULL,
 "主任医师工号" varchar (20) DEFAULT NULL,
 "主治医师姓名" varchar (50) DEFAULT NULL,
 "主治医师工号" varchar (20) DEFAULT NULL,
 "住院医师姓名" varchar (50) DEFAULT NULL,
 "住院医师工号" varchar (20) DEFAULT NULL,
 "接诊医师姓名" varchar (50) DEFAULT NULL,
 "接诊医师工号" varchar (20) DEFAULT NULL,
 "治则治法代码" varchar (100) DEFAULT NULL,
 "中医“四诊”观察结果" text,
 "死亡诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "死亡诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "死亡诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "死亡诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "死亡诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "死亡诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "入院诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "入院诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "入院诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "入院诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "入院诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "入院诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "死亡原因" text,
 "诊疗过程描述" text,
 "入院情况" text,
 "死亡时间" timestamp DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "陈述内容可靠标志" varchar (1) DEFAULT NULL,
 "陈述者与患者的关系名称" varchar (50) DEFAULT NULL,
 "陈述者与患者的关系代码" varchar (1) DEFAULT NULL,
 "病史陈述者姓名" varchar (50) DEFAULT NULL,
 "职业类别名称" varchar (60) DEFAULT NULL,
 "职业类别代码" varchar (4) DEFAULT NULL,
 "现住详细地址" varchar (128) DEFAULT NULL,
 "现住址-门牌号码" varchar (70) DEFAULT NULL,
 "现住址-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "现住址-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "现地址-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "现住址-县(市、区)名称" varchar (70) DEFAULT NULL,
 "现住址-县(市、区)代码" varchar (20) DEFAULT NULL,
 "现住址-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "现住址-市(地区、州)代码" varchar (20) DEFAULT NULL,
 "现住址-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "现住址-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "现住址行政区划代码" varchar (12) DEFAULT NULL,
 "婚姻状况名称" varchar (50) DEFAULT NULL,
 "婚姻状况代码" varchar (2) DEFAULT NULL,
 "民族名称" varchar (50) DEFAULT NULL,
 CONSTRAINT "24H内入院死亡记录"_"医疗机构代码"_"住院死亡记录流水号"_"住院就诊流水号"_PK PRIMARY KEY ("医疗机构代码",
 "住院死亡记录流水号",
 "住院就诊流水号")
);
COMMENT ON COLUMN "24h内入院死亡记录"."民族名称" IS '所属民族的名称';
COMMENT ON COLUMN "24h内入院死亡记录"."婚姻状况代码" IS '当前婚姻状况(已婚、未婚、初婚等)在标准特定编码体系中的代码';
COMMENT ON COLUMN "24h内入院死亡记录"."婚姻状况名称" IS '当前婚姻状况(已婚、未婚、初婚等)在标准特定编码体系中的名称';
COMMENT ON COLUMN "24h内入院死亡记录"."现住址行政区划代码" IS '现住地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "24h内入院死亡记录"."现住址-省(自治区、直辖市)代码" IS '个人现住地址的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入院死亡记录"."现住址-省(自治区、直辖市)名称" IS '个人现住地址的省、自治区或直辖市名称';
COMMENT ON COLUMN "24h内入院死亡记录"."现住址-市(地区、州)代码" IS '个人现住地址的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入院死亡记录"."现住址-市(地区、州)名称" IS '个人现住地址的市、地区或州的名称';
COMMENT ON COLUMN "24h内入院死亡记录"."现住址-县(市、区)代码" IS '个人现住地址的县(市、区)的在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入院死亡记录"."现住址-县(市、区)名称" IS '个人现住地址的县(市、区)的名称';
COMMENT ON COLUMN "24h内入院死亡记录"."现地址-乡(镇、街道办事处)代码" IS '个人现住地址的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入院死亡记录"."现住址-乡(镇、街道办事处)名称" IS '个人现住地址的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "24h内入院死亡记录"."现住址-村(街、路、弄等)名称" IS '个人现住地址的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "24h内入院死亡记录"."现住址-门牌号码" IS '个人现住地址的门牌号码';
COMMENT ON COLUMN "24h内入院死亡记录"."现住详细地址" IS '个人现住地址的详细地址';
COMMENT ON COLUMN "24h内入院死亡记录"."职业类别代码" IS '本人从事职业所属类别(如国家公务员、专业技术人员、职员、工人等)在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入院死亡记录"."职业类别名称" IS '本人从事职业所属类别(如国家公务员、专业技术人员、职员、工人等)在特定编码体系中的名称';
COMMENT ON COLUMN "24h内入院死亡记录"."病史陈述者姓名" IS '患者病史的陈述人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入院死亡记录"."陈述者与患者的关系代码" IS '患者病史陈述人与患者的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入院死亡记录"."陈述者与患者的关系名称" IS '患者病史陈述人与患者的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的名称';
COMMENT ON COLUMN "24h内入院死亡记录"."陈述内容可靠标志" IS '标识陈述内容是否可信的标志';
COMMENT ON COLUMN "24h内入院死亡记录"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "24h内入院死亡记录"."死亡时间" IS '死亡时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "24h内入院死亡记录"."入院情况" IS '对患者入院情况的详细描述';
COMMENT ON COLUMN "24h内入院死亡记录"."诊疗过程描述" IS '对患者诊疗过程或抢救情况的详细描述';
COMMENT ON COLUMN "24h内入院死亡记录"."死亡原因" IS '患者死亡直接原因的详细描述';
COMMENT ON COLUMN "24h内入院死亡记录"."入院诊断-西医诊断代码" IS '患者入院时按照平台编码规则赋予西医入院诊断疾病的唯一标识';
COMMENT ON COLUMN "24h内入院死亡记录"."入院诊断-西医诊断名称" IS '由医师根据患者入院时的情况，综合分析所作出的西医诊断标准名称';
COMMENT ON COLUMN "24h内入院死亡记录"."入院诊断-中医病名代码" IS '患者入院时按照平台编码规则赋予入院诊断中医疾病的唯一标识';
COMMENT ON COLUMN "24h内入院死亡记录"."入院诊断-中医病名名称" IS '由医师根据患者人院时的情况，综合分析所作出的中医疾病标准名称';
COMMENT ON COLUMN "24h内入院死亡记录"."入院诊断-中医症候代码" IS '患者入院时按照平台编码规则赋予入院诊断中医证候的唯一标识';
COMMENT ON COLUMN "24h内入院死亡记录"."入院诊断-中医症候名称" IS '由医师根据患者人院时的情况，综合分析所作出的标准中医证候名称';
COMMENT ON COLUMN "24h内入院死亡记录"."死亡诊断-西医诊断代码" IS '按照平台编码规则赋予西医死亡诊断疾病的唯一标识';
COMMENT ON COLUMN "24h内入院死亡记录"."死亡诊断-西医诊断名称" IS '死亡诊断西医诊断标准名称';
COMMENT ON COLUMN "24h内入院死亡记录"."死亡诊断-中医病名代码" IS '患者入院时按照特定编码规则赋予死亡诊断中医疾病的唯一标识';
COMMENT ON COLUMN "24h内入院死亡记录"."死亡诊断-中医病名名称" IS '患者入院时按照平台编码规则赋予死亡诊断中医疾病的唯一标识';
COMMENT ON COLUMN "24h内入院死亡记录"."死亡诊断-中医症候代码" IS '患者入院时按照平台编码规则赋予死亡诊断中医证候的唯一标识';
COMMENT ON COLUMN "24h内入院死亡记录"."死亡诊断-中医症候名称" IS '死亡诊断中医证候标准名称';
COMMENT ON COLUMN "24h内入院死亡记录"."中医“四诊”观察结果" IS '中医“四诊”观察结果的详细描述，包括望、闻问、切四诊内容';
COMMENT ON COLUMN "24h内入院死亡记录"."治则治法代码" IS '根据辨证结果采用的治则治法名称术语在编码体系中的代码';
COMMENT ON COLUMN "24h内入院死亡记录"."接诊医师工号" IS '接诊医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "24h内入院死亡记录"."接诊医师姓名" IS '接诊医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入院死亡记录"."住院医师工号" IS '所在科室具体负责诊治的，具有住院医师的工号';
COMMENT ON COLUMN "24h内入院死亡记录"."住院医师姓名" IS '所在科室具体负责诊治的，具有住院医师专业技术职务任职资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入院死亡记录"."主治医师工号" IS '所在科室的具有主治医师的工号';
COMMENT ON COLUMN "24h内入院死亡记录"."主治医师姓名" IS '所在科室的具有主治医师专业技术职务资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入院死亡记录"."主任医师工号" IS '主任医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "24h内入院死亡记录"."主任医师姓名" IS '主任医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入院死亡记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "24h内入院死亡记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "24h内入院死亡记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "24h内入院死亡记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "24h内入院死亡记录"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "24h内入院死亡记录"."住院死亡记录流水号" IS '按照某一特性编码规则赋予24小时内入院死亡记录唯一标志的顺序号';
COMMENT ON COLUMN "24h内入院死亡记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "24h内入院死亡记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "24h内入院死亡记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "24h内入院死亡记录"."科室代码" IS '科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在机构内编码体系中的代码';
COMMENT ON COLUMN "24h内入院死亡记录"."科室名称" IS '患者入院时的科室的机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "24h内入院死亡记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "24h内入院死亡记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入院死亡记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "24h内入院死亡记录"."住院次数" IS '患者在医院住院的次数';
COMMENT ON COLUMN "24h内入院死亡记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入院死亡记录"."性别代码" IS '患者生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入院死亡记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "24h内入院死亡记录"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "24h内入院死亡记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "24h内入院死亡记录"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON TABLE "24h内入院死亡记录" IS '患者入院不足24小时内死亡的入院记录';


COMMENT ON TABLE "24h内入出院记录" IS '患者入院不足24小时出院的入院记录';
COMMENT ON COLUMN "24h内入出院记录"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入出院记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "24h内入出院记录"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "24h内入出院记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "24h内入出院记录"."性别名称" IS '患者生理性别在特定编码体系中的名称';
COMMENT ON COLUMN "24h内入出院记录"."性别代码" IS '患者生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入出院记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入出院记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "24h内入出院记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入出院记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "24h内入出院记录"."就诊次数" IS '患者在医院就诊的次数';
COMMENT ON COLUMN "24h内入出院记录"."科室名称" IS '患者入院时的科室的机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "24h内入出院记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "24h内入出院记录"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "24h内入出院记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "24h内入出院记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "24h内入出院记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "24h内入出院记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "24h内入出院记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "24h内入出院记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "24h内入出院记录"."主任医师姓名" IS '主任医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入出院记录"."主任医师工号" IS '主任医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "24h内入出院记录"."主治医师姓名" IS '所在科室的具有主治医师专业技术职务资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入出院记录"."主治医师工号" IS '所在科室的具有主治医师的工号';
COMMENT ON COLUMN "24h内入出院记录"."住院医师姓名" IS '所在科室具体负责诊治的，具有住院医师专业技术职务任职资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入出院记录"."住院医师工号" IS '所在科室具体负责诊治的，具有住院医师的工号';
COMMENT ON COLUMN "24h内入出院记录"."接诊医师姓名" IS '接诊医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入出院记录"."接诊医师工号" IS '接诊医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "24h内入出院记录"."出院医嘱开立时间" IS '出院医嘱开立时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "24h内入出院记录"."出院医嘱开立人姓名" IS '出院医嘱开立医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入出院记录"."出院医嘱" IS '对患者出院医嘱的详细描述';
COMMENT ON COLUMN "24h内入出院记录"."出院情况" IS '对患者出院情况的详细描述';
COMMENT ON COLUMN "24h内入出院记录"."出院诊断-中医症候名称" IS '出院诊断中医证候标准名称';
COMMENT ON COLUMN "24h内入出院记录"."出院诊断-中医症候代码" IS '患者入院时按照平台编码规则赋予出院诊断中医证候的唯一标识';
COMMENT ON COLUMN "24h内入出院记录"."出院诊断-中医病名名称" IS '患者入院时按照平台编码规则赋予出院诊断中医疾病的唯一标识';
COMMENT ON COLUMN "24h内入出院记录"."出院诊断-中医病名代码" IS '患者入院时按照特定编码规则赋予出院诊断中医疾病的唯一标识';
COMMENT ON COLUMN "24h内入出院记录"."出院诊断-西医诊断名称" IS '出院诊断西医诊断疾病标准名称';
COMMENT ON COLUMN "24h内入出院记录"."出院诊断-西医诊断代码" IS '按照平台编码规则赋予西医出院诊断疾病的唯一标识';
COMMENT ON COLUMN "24h内入出院记录"."入院诊断-中医症候名称" IS '由医师根据患者人院时的情况，综合分析所作出的标准中医证候名称';
COMMENT ON COLUMN "24h内入出院记录"."入院诊断-中医症候代码" IS '患者入院时按照平台编码规则赋予入院诊断中医证候的唯一标识';
COMMENT ON COLUMN "24h内入出院记录"."入院诊断-中医病名名称" IS '由医师根据患者人院时的情况，综合分析所作出的中医疾病标准名称';
COMMENT ON COLUMN "24h内入出院记录"."入院诊断-中医病名代码" IS '患者入院时按照平台编码规则赋予入院诊断中医疾病的唯一标识';
COMMENT ON COLUMN "24h内入出院记录"."入院诊断-西医诊断名称" IS '由医师根据患者入院时的情况，综合分析所作出的西医诊断标准名称';
COMMENT ON COLUMN "24h内入出院记录"."入院诊断-西医诊断代码" IS '患者入院时按照平台编码规则赋予西医入院诊断疾病的唯一标识';
COMMENT ON COLUMN "24h内入出院记录"."治则治法代码" IS '辩证结果采用的治则治法在特定编码体系中的代码。如有多条，用“，”加以分隔';
COMMENT ON COLUMN "24h内入出院记录"."中医“四诊”观察结果" IS '中医“四诊”观察结果的详细描述，包括望、闻、问、切四诊内容';
COMMENT ON COLUMN "24h内入出院记录"."诊疗过程描述" IS '对患者诊疗过程或抢救情况的详细描述';
COMMENT ON COLUMN "24h内入出院记录"."症状描述" IS '对个体出现症状的详细描述';
COMMENT ON COLUMN "24h内入出院记录"."症状名称" IS '病人复发时症状的名称';
COMMENT ON COLUMN "24h内入出院记录"."入院情况" IS '对患者入院情况的详细描述';
COMMENT ON COLUMN "24h内入出院记录"."现病史" IS '对患者当前所患疾病情况的详细描述';
COMMENT ON COLUMN "24h内入出院记录"."主诉" IS '对患者本次疾病相关的主要症状及其持续时间的描述，一般由患者本人或监护人描述';
COMMENT ON COLUMN "24h内入出院记录"."出院时间" IS '患者实际办理出院手续时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "24h内入出院记录"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "24h内入出院记录"."陈述内容可靠标志" IS '标识陈述内容是否可信的标志';
COMMENT ON COLUMN "24h内入出院记录"."陈述者与患者的关系名称" IS '患者病史陈述人与患者的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的名称';
COMMENT ON COLUMN "24h内入出院记录"."陈述者与患者的关系代码" IS '患者病史陈述人与患者的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入出院记录"."病史陈述者姓名" IS '患者病史的陈述人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "24h内入出院记录"."职业类别名称" IS '本人从事职业所属类别(如国家公务员、专业技术人员、职员、工人等)在特定编码体系中的名称';
COMMENT ON COLUMN "24h内入出院记录"."职业类别代码" IS '本人从事职业所属类别(如国家公务员、专业技术人员、职员、工人等)在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入出院记录"."现住详细地址" IS '个人现住地址的详细地址';
COMMENT ON COLUMN "24h内入出院记录"."现住址-门牌号码" IS '个人现住地址的门牌号码';
COMMENT ON COLUMN "24h内入出院记录"."现住址-村(街、路、弄等)名称" IS '个人现住地址的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "24h内入出院记录"."现住址-乡(镇、街道办事处)名称" IS '个人现住地址的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "24h内入出院记录"."现地址-乡(镇、街道办事处)代码" IS '个人现住地址的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入出院记录"."现住址-县(市、区)名称" IS '个人现住地址的县(市、区)的名称';
COMMENT ON COLUMN "24h内入出院记录"."现住址-县(市、区)代码" IS '个人现住地址的县(市、区)的在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入出院记录"."现住址-市(地区、州)名称" IS '个人现住地址的市、地区或州的名称';
COMMENT ON COLUMN "24h内入出院记录"."现住址-市(地区、州)代码" IS '个人现住地址的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入出院记录"."现住址-省(自治区、直辖市)名称" IS '个人现住地址的省、自治区或直辖市名称';
COMMENT ON COLUMN "24h内入出院记录"."现住址-省(自治区、直辖市)代码" IS '个人现住地址的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "24h内入出院记录"."现住址行政区划代码" IS '现住地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "24h内入出院记录"."婚姻状况名称" IS '当前婚姻状况(已婚、未婚、初婚等)在标准特定编码体系中的名称';
COMMENT ON COLUMN "24h内入出院记录"."婚姻状况代码" IS '当前婚姻状况(已婚、未婚、初婚等)在标准特定编码体系中的代码';
COMMENT ON COLUMN "24h内入出院记录"."民族名称" IS '所属民族的名称';
CREATE TABLE IF NOT EXISTS "24h内入出院记录" (
"民族代码" varchar (2) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "住院就诊流水号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "主任医师姓名" varchar (50) DEFAULT NULL,
 "主任医师工号" varchar (20) DEFAULT NULL,
 "主治医师姓名" varchar (50) DEFAULT NULL,
 "主治医师工号" varchar (20) DEFAULT NULL,
 "住院医师姓名" varchar (50) DEFAULT NULL,
 "住院医师工号" varchar (20) DEFAULT NULL,
 "接诊医师姓名" varchar (50) DEFAULT NULL,
 "接诊医师工号" varchar (20) DEFAULT NULL,
 "出院医嘱开立时间" timestamp DEFAULT NULL,
 "出院医嘱开立人姓名" varchar (50) DEFAULT NULL,
 "出院医嘱" text,
 "出院情况" text,
 "出院诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "出院诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "出院诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "出院诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "出院诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "出院诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "入院诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "入院诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "入院诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "入院诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "入院诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "入院诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "治则治法代码" varchar (100) DEFAULT NULL,
 "中医“四诊”观察结果" text,
 "诊疗过程描述" text,
 "症状描述" varchar (1000) DEFAULT NULL,
 "症状名称" varchar (50) DEFAULT NULL,
 "入院情况" text,
 "现病史" text,
 "主诉" text,
 "出院时间" timestamp DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "陈述内容可靠标志" varchar (1) DEFAULT NULL,
 "陈述者与患者的关系名称" varchar (50) DEFAULT NULL,
 "陈述者与患者的关系代码" varchar (1) DEFAULT NULL,
 "病史陈述者姓名" varchar (50) DEFAULT NULL,
 "职业类别名称" varchar (60) DEFAULT NULL,
 "职业类别代码" varchar (4) DEFAULT NULL,
 "现住详细地址" varchar (128) DEFAULT NULL,
 "现住址-门牌号码" varchar (70) DEFAULT NULL,
 "现住址-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "现住址-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "现地址-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "现住址-县(市、区)名称" varchar (70) DEFAULT NULL,
 "现住址-县(市、区)代码" varchar (20) DEFAULT NULL,
 "现住址-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "现住址-市(地区、州)代码" varchar (20) DEFAULT NULL,
 "现住址-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "现住址-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "现住址行政区划代码" varchar (12) DEFAULT NULL,
 "婚姻状况名称" varchar (50) DEFAULT NULL,
 "婚姻状况代码" varchar (2) DEFAULT NULL,
 "民族名称" varchar (50) DEFAULT NULL,
 CONSTRAINT "24H内入出院记录"_"住院就诊流水号"_"医疗机构代码"_PK PRIMARY KEY ("住院就诊流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "(个人)基本信息" IS '个人基本信息，包括证件号码、姓名、性别、国籍、民族、建档信息等';
COMMENT ON COLUMN "(个人)基本信息"."职业类别代码" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."婚姻状况代码" IS '当前婚姻状况(已婚、未婚、初婚等)在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."婚姻状况名称" IS '当前婚姻状况的标准名称，如已婚、未婚、初婚等';
COMMENT ON COLUMN "(个人)基本信息"."医疗费用支付方式代码" IS '患者医疗费用支付方式类别(如城镇职工基本医疗保险、城镇居民基本医疗保险等)在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."医疗费用支付方式名称" IS '患者医疗费用支付方式类别在特定编码体系中的名称，如城镇职工基本医疗保险、城镇居民基本医疗保险等';
COMMENT ON COLUMN "(个人)基本信息"."医疗费用支付方式其他" IS '除上述医疗费用支付方式外，其他支付方式的名称';
COMMENT ON COLUMN "(个人)基本信息"."慢性病患病情况代码" IS '慢性病患病情况在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."药物过敏史标志" IS '标识患者有无药物过敏经历的标志';
COMMENT ON COLUMN "(个人)基本信息"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(个人)基本信息"."药物过敏代码" IS '诱发个体过敏性疾病的药物在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."药物过敏名称" IS '诱发个体过敏性疾病的药物在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)基本信息"."药物过敏其他" IS '除上述药物过敏源外，其他诱发个体过敏性疾病的药物名称';
COMMENT ON COLUMN "(个人)基本信息"."过敏发生时间" IS '过敏情况发生时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)基本信息"."暴露史代码" IS '个体接触环境危险因素在特定编码体系中的类别代码';
COMMENT ON COLUMN "(个人)基本信息"."暴露史名称" IS '个体接触环境危险因素的类别标准名称，如化学品、毒物、射线等';
COMMENT ON COLUMN "(个人)基本信息"."暴露史其他" IS '其他环境危险因素名称的详细描述';
COMMENT ON COLUMN "(个人)基本信息"."手术史标志" IS '标识本人既往有无手术经历的标志';
COMMENT ON COLUMN "(个人)基本信息"."外伤史标志" IS '标识本人既往有无外伤的标志';
COMMENT ON COLUMN "(个人)基本信息"."输血史标志" IS '标识本人是否输过血的标志';
COMMENT ON COLUMN "(个人)基本信息"."父亲病史代码" IS '父亲慢性病患病情况在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."父亲病史名称" IS '父亲慢性病患病情况在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)基本信息"."父亲病史其他" IS '除上述病史外，父亲患慢性病其他情况的额描述';
COMMENT ON COLUMN "(个人)基本信息"."母亲病史代码" IS '母亲慢性病患病情况在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."母亲病史名称" IS '母亲慢性病患病情况在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)基本信息"."母亲病史其他" IS '除上述病史外，母亲患慢性病其他情况的额描述';
COMMENT ON COLUMN "(个人)基本信息"."兄弟姐妹病史代码" IS '兄弟姐妹慢性病患病情况在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."兄弟姐妹病史名称" IS '兄弟姐妹慢性病患病情况在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)基本信息"."兄弟姐妹病史其他" IS '除上述病史外，兄弟姐妹患慢性病其他情况的额描述';
COMMENT ON COLUMN "(个人)基本信息"."子女病史代码" IS '子女慢性病患病情况在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."子女病史名称" IS '子女慢性病患病情况在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)基本信息"."子女病史其他" IS '除上述病史外，子女患慢性病其他情况的额描述';
COMMENT ON COLUMN "(个人)基本信息"."遗传病史标志" IS '标识患者是否有遗传病史的标志';
COMMENT ON COLUMN "(个人)基本信息"."遗传疾病名称" IS '遗传疾病诊断在特定编码体系中的名称。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "(个人)基本信息"."残疾标志" IS '标识患者是否残疾的标志';
COMMENT ON COLUMN "(个人)基本信息"."残疾代码" IS '残疾种类在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."残疾名称" IS '残疾种类在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)基本信息"."残疾其他" IS '除上述残疾种类外，其他残疾情况的名称';
COMMENT ON COLUMN "(个人)基本信息"."厨房排风设施标志" IS '标识有无家庭厨房排风设施的标志';
COMMENT ON COLUMN "(个人)基本信息"."厨房排风设施类别代码" IS '本人家庭中所使用的厨房排风设施的类别(如油烟机、换气扇、烟囱等)在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."厨房排风设施类别名称" IS '本人家庭中所使用的厨房排风设施的类别标准名称，如油烟机、换气扇、烟囱等';
COMMENT ON COLUMN "(个人)基本信息"."燃料类型代码" IS '本人家庭中所使用的燃料类别(如液化气、煤、天然气、沼气、柴火等)在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."燃料类型名称" IS '本人家庭中所使用的燃料类别的标准名称，如液化气、煤、天然气、沼气、柴火等';
COMMENT ON COLUMN "(个人)基本信息"."饮水类别代码" IS '家庭饮水的类别(如自来水、经净化过滤的水、井水、河湖水、塘水等)在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."饮水类别名称" IS '家庭饮水的类别的标准名称，如自来水、经净化过滤的水、井水、河湖水、塘水等';
COMMENT ON COLUMN "(个人)基本信息"."厕所类别代码" IS '本人家庭中所使用的厕所类别(如卫生厕所、马桶、露天粪坑、简易棚厕等)在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."厕所类别名称" IS '本人家庭中所使用的厕所的类别的标准名称，如卫生厕所、马桶、露天粪坑、简易棚厕等';
COMMENT ON COLUMN "(个人)基本信息"."禽畜栏类别代码" IS '家庭设立禽畜养殖场所的类别(如单设、室内、室外等)在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."禽畜栏类别名称" IS '家庭设立禽畜养殖场所的类别的标准名称，如单设、室内、室外等';
COMMENT ON COLUMN "(个人)基本信息"."出生地-详细地址" IS '个人出生地址的详细描述';
COMMENT ON COLUMN "(个人)基本信息"."出生地-行政区划代码" IS '出生地址的6位区县行政区划码+省统计局发布的3位乡镇编码和3位村编码';
COMMENT ON COLUMN "(个人)基本信息"."出生地-地址代码" IS '出生地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(个人)基本信息"."出生地-省(自治区、直辖市)代码" IS '个人出生时所在地址的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."出生地-省(自治区、直辖市)名称" IS '个人出生时所在地址的省、自治区或直辖市名称';
COMMENT ON COLUMN "(个人)基本信息"."出生地-市(地区、州)代码" IS '个人出生时所在地址的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."出生地-市(地区、州)名称" IS '个人出生时所在地址的市、地区或州的名称';
COMMENT ON COLUMN "(个人)基本信息"."出生地-县(市、区)代码" IS '个人出生时所在地址的县(市、区)的在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."出生地-县(市、区)名称" IS '个人出生时所在地址的县(市、区)的名称';
COMMENT ON COLUMN "(个人)基本信息"."出生地-乡(镇、街道办事处)代码" IS '个人出生时所在地址的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."出生地-乡(镇、街道办事处)名称" IS '个人出生时所在地址的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(个人)基本信息"."出生地-村(街、路、弄等)代码" IS '个人出生时所在地址的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."出生地-村(街、路、弄等)名称" IS '个人出生时所在地址的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(个人)基本信息"."出生地-门牌号码" IS '个人出生时所在地址的门牌号码';
COMMENT ON COLUMN "(个人)基本信息"."出生地-邮政编码" IS '个人出生时所在地址的邮政编码';
COMMENT ON COLUMN "(个人)基本信息"."居住地-详细地址" IS '现住地址的详细描述';
COMMENT ON COLUMN "(个人)基本信息"."居住地-行政区划代码" IS '居住地址的6位区县行政区划码+省统计局发布的3位乡镇编码和3位村编码';
COMMENT ON COLUMN "(个人)基本信息"."居住地-省(自治区、直辖市)代码" IS '现住地址中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."居住地-省(自治区、直辖市)名称" IS '现住地址中的省、自治区或直辖市名称';
COMMENT ON COLUMN "(个人)基本信息"."居住地-市(地区、州)代码" IS '现住地址中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."居住地-市(地区、州)名称" IS '现住地址中的市、地区或州的名称';
COMMENT ON COLUMN "(个人)基本信息"."居住地-县(市、区)代码" IS '现住地址中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."居住地-县(市、区)名称" IS '现住地址中的县或区名称';
COMMENT ON COLUMN "(个人)基本信息"."居住地-乡(镇、街道办事处)代码" IS '本人现住地址中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."居住地-乡(镇、街道办事处)名称" IS '本人现住地址中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(个人)基本信息"."居住地-村(街、路、弄等)代码" IS '本人现住地址中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."居住地-村(街、路、弄等)名称" IS '本人现住地址中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(个人)基本信息"."居住地-门牌号码" IS '本人现住地址中的门牌号码';
COMMENT ON COLUMN "(个人)基本信息"."居住地-邮政编码" IS '现住地址中所在行政区的邮政编码';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-详细地址" IS '户籍地址的详细描述';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-行政区划代码" IS '户籍地址的6位区县行政区划码+省统计局发布的3位乡镇编码和3位村编码';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-省(自治区、直辖市)代码" IS '户籍登记所在地址的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-省(自治区、直辖市)名称" IS '户籍登记所在地址的省、自治区或直辖市名称';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-市(地区、州)代码" IS '户籍登记所在地址的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-市(地区、州)名称" IS '户籍登记所在地址的市、地区或州的名称';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-县(市、区)代码" IS '户籍登记所在地址的县(区)的在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-县(市、区)名称" IS '户籍登记所在地址的县(市、区)的名称';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-乡(镇、街道办事处)代码" IS '户籍登记所在地址的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-乡(镇、街道办事处)名称" IS '户籍登记所在地址的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-村(街、路、弄等)代码" IS '户籍登记所在地址的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-村(街、路、弄等)名称" IS '户籍登记所在地址的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-门牌号码" IS '户籍登记所在地址的门牌号码';
COMMENT ON COLUMN "(个人)基本信息"."户籍地-邮政编码" IS '户籍地址所在行政区的邮政编码';
COMMENT ON COLUMN "(个人)基本信息"."紧急情况联系人" IS '紧急联系人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个人)基本信息"."紧急情况联系人电话" IS '紧急联系人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(个人)基本信息"."档案合格标志" IS '标识个人健康档案是否质量合格的标志';
COMMENT ON COLUMN "(个人)基本信息"."档案完善标志" IS '标识个人健康档案是否内容完善的标志';
COMMENT ON COLUMN "(个人)基本信息"."档案管理标志" IS '标识个人健康档案是否进行管理的标志';
COMMENT ON COLUMN "(个人)基本信息"."死亡标志" IS '标识个体是否死亡的标志';
COMMENT ON COLUMN "(个人)基本信息"."死亡日期" IS '患者死亡当日的公元纪年日期';
COMMENT ON COLUMN "(个人)基本信息"."死亡原因" IS '患者死亡原因的详细描述';
COMMENT ON COLUMN "(个人)基本信息"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)基本信息"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(个人)基本信息"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个人)基本信息"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(个人)基本信息"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个人)基本信息"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)基本信息"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(个人)基本信息"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个人)基本信息"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(个人)基本信息"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个人)基本信息"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(个人)基本信息"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)基本信息"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)基本信息"."职业类别名称" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)基本信息"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(个人)基本信息"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(个人)基本信息"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(个人)基本信息"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."卡类型名称" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)基本信息"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "(个人)基本信息"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)基本信息"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "(个人)基本信息"."档案编号" IS '按照某一特定编码规则赋予个体城乡居民健康档案的编号';
COMMENT ON COLUMN "(个人)基本信息"."建档机构代码" IS '建档医疗机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(个人)基本信息"."建档机构名称" IS '建档医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个人)基本信息"."建档机构联系电话" IS '建档机构的联系电话，包括区号';
COMMENT ON COLUMN "(个人)基本信息"."建档医生工号" IS '首次为患者建立电子病历的人员的工号';
COMMENT ON COLUMN "(个人)基本信息"."建档医生姓名" IS '首次为患者建立电子病历的人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个人)基本信息"."建档日期" IS '建档完成时公元纪年日期的完整描述';
COMMENT ON COLUMN "(个人)基本信息"."档案管理机构代码" IS '按照机构内编码规则赋予档案管理机构的唯一标识';
COMMENT ON COLUMN "(个人)基本信息"."档案管理机构名称" IS '档案管理机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个人)基本信息"."健康卡发卡机构代码" IS '按照特定编码规则赋予健康卡发卡机构机构的唯一标识';
COMMENT ON COLUMN "(个人)基本信息"."健康卡发卡机构名称" IS '健康卡发卡机构机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个人)基本信息"."责任医生工号" IS '就诊医师在原始特定编码体系中的编号，对于住院业务填写入院诊断医师工号';
COMMENT ON COLUMN "(个人)基本信息"."责任医生姓名" IS '就诊医师在公安户籍管理部门正式登记注册的姓氏和名称，对于住院业务填写入院诊断医师姓名';
COMMENT ON COLUMN "(个人)基本信息"."责任医生联系电话" IS '责任医师的联系电话，包括区号';
COMMENT ON COLUMN "(个人)基本信息"."姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个人)基本信息"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "(个人)基本信息"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "(个人)基本信息"."个人照片" IS '用于帮助识别身份且有固定大小的真实图片的路径';
COMMENT ON COLUMN "(个人)基本信息"."工作单位名称" IS '个体工作单位的组织机构名称';
COMMENT ON COLUMN "(个人)基本信息"."工作单位联系电话" IS '所在的工作单位的电话号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(个人)基本信息"."本人电话号码" IS '本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(个人)基本信息"."电子邮件地址" IS '本人的电子邮箱名称';
COMMENT ON COLUMN "(个人)基本信息"."联系人关系代码" IS '联系人与患者之间的关系类别(如配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."联系人关系名称" IS '联系人与患者之间的关系类别的标准名称，如配偶、子女、父母等';
COMMENT ON COLUMN "(个人)基本信息"."联系人姓名" IS '联系人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个人)基本信息"."联系人电话号码" IS '联系人的电话号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(个人)基本信息"."常住地址户籍标志" IS '标识个体的常住地址是否为户籍所在地的标志';
COMMENT ON COLUMN "(个人)基本信息"."常住人口标志" IS '标识个体为常住人口的标志';
COMMENT ON COLUMN "(个人)基本信息"."国籍代码" IS '所属国籍在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."国籍名称" IS '所属国籍在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)基本信息"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."民族名称" IS '所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)基本信息"."ABO血型代码" IS '受检者按照ABO血型系统决定的血型在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."ABO血型名称" IS '受检者按照ABO血型系统决定的血型的标准名称';
COMMENT ON COLUMN "(个人)基本信息"."Rh血型代码" IS '进行血型检查明确，或既往病历资料能够明确的患者Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."Rh血型名称" IS '进行血型检查明确，或既往病历资料能够明确的患者Rh血型的类别在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)基本信息"."学历代码" IS '个体受教育最高程度的类别(如研究生教育、大学本科、专科教育等)在特定编码体系中的代码';
COMMENT ON COLUMN "(个人)基本信息"."学历名称" IS '个体受教育最高程度的类别标准名称，如研究生教育、大学本科、专科教育等';
CREATE TABLE IF NOT EXISTS "(
个人)基本信息" ("职业类别代码" varchar (4) DEFAULT NULL,
 "婚姻状况代码" varchar (2) DEFAULT NULL,
 "婚姻状况名称" varchar (50) DEFAULT NULL,
 "医疗费用支付方式代码" varchar (30) DEFAULT NULL,
 "医疗费用支付方式名称" varchar (100) DEFAULT NULL,
 "医疗费用支付方式其他" varchar (100) DEFAULT NULL,
 "慢性病患病情况代码" varchar (30) DEFAULT NULL,
 "药物过敏史标志" varchar (1) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "药物过敏代码" varchar (30) DEFAULT NULL,
 "药物过敏名称" varchar (100) DEFAULT NULL,
 "药物过敏其他" varchar (100) DEFAULT NULL,
 "过敏发生时间" timestamp DEFAULT NULL,
 "暴露史代码" varchar (30) DEFAULT NULL,
 "暴露史名称" varchar (100) DEFAULT NULL,
 "暴露史其他" varchar (100) DEFAULT NULL,
 "手术史标志" varchar (1) DEFAULT NULL,
 "外伤史标志" varchar (1) DEFAULT NULL,
 "输血史标志" varchar (1) DEFAULT NULL,
 "父亲病史代码" varchar (60) DEFAULT NULL,
 "父亲病史名称" varchar (100) DEFAULT NULL,
 "父亲病史其他" varchar (100) DEFAULT NULL,
 "母亲病史代码" varchar (60) DEFAULT NULL,
 "母亲病史名称" varchar (100) DEFAULT NULL,
 "母亲病史其他" varchar (100) DEFAULT NULL,
 "兄弟姐妹病史代码" varchar (60) DEFAULT NULL,
 "兄弟姐妹病史名称" varchar (100) DEFAULT NULL,
 "兄弟姐妹病史其他" varchar (100) DEFAULT NULL,
 "子女病史代码" varchar (60) DEFAULT NULL,
 "子女病史名称" varchar (100) DEFAULT NULL,
 "子女病史其他" varchar (100) DEFAULT NULL,
 "遗传病史标志" varchar (1) DEFAULT NULL,
 "遗传疾病名称" varchar (100) DEFAULT NULL,
 "残疾标志" varchar (1) DEFAULT NULL,
 "残疾代码" varchar (30) DEFAULT NULL,
 "残疾名称" varchar (100) DEFAULT NULL,
 "残疾其他" varchar (100) DEFAULT NULL,
 "厨房排风设施标志" varchar (1) DEFAULT NULL,
 "厨房排风设施类别代码" varchar (2) DEFAULT NULL,
 "厨房排风设施类别名称" varchar (50) DEFAULT NULL,
 "燃料类型代码" varchar (2) DEFAULT NULL,
 "燃料类型名称" varchar (50) DEFAULT NULL,
 "饮水类别代码" varchar (2) DEFAULT NULL,
 "饮水类别名称" varchar (50) DEFAULT NULL,
 "厕所类别代码" varchar (2) DEFAULT NULL,
 "厕所类别名称" varchar (50) DEFAULT NULL,
 "禽畜栏类别代码" varchar (2) DEFAULT NULL,
 "禽畜栏类别名称" varchar (50) DEFAULT NULL,
 "出生地-详细地址" varchar (200) DEFAULT NULL,
 "出生地-行政区划代码" varchar (12) DEFAULT NULL,
 "出生地-地址代码" varchar (9) DEFAULT NULL,
 "出生地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "出生地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "出生地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "出生地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "出生地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "出生地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "出生地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "出生地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "出生地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "出生地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "出生地-门牌号码" varchar (70) DEFAULT NULL,
 "出生地-邮政编码" varchar (6) DEFAULT NULL,
 "居住地-详细地址" varchar (200) DEFAULT NULL,
 "居住地-行政区划代码" varchar (12) DEFAULT NULL,
 "居住地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "居住地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "居住地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "居住地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "居住地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "居住地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "居住地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "居住地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "居住地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "居住地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "居住地-门牌号码" varchar (70) DEFAULT NULL,
 "居住地-邮政编码" varchar (6) DEFAULT NULL,
 "户籍地-详细地址" varchar (200) DEFAULT NULL,
 "户籍地-行政区划代码" varchar (12) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "户籍地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "户籍地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "户籍地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "户籍地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "户籍地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "户籍地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "户籍地-门牌号码" varchar (100) DEFAULT NULL,
 "户籍地-邮政编码" varchar (6) DEFAULT NULL,
 "紧急情况联系人" varchar (50) DEFAULT NULL,
 "紧急情况联系人电话" varchar (20) DEFAULT NULL,
 "档案合格标志" varchar (1) DEFAULT NULL,
 "档案完善标志" varchar (1) DEFAULT NULL,
 "档案管理标志" varchar (1) DEFAULT NULL,
 "死亡标志" varchar (1) DEFAULT NULL,
 "死亡日期" date DEFAULT NULL,
 "死亡原因" text,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "职业类别名称" varchar (60) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "个人唯一标识号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡类型名称" varchar (50) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "档案编号" varchar (20) DEFAULT NULL,
 "建档机构代码" varchar (22) DEFAULT NULL,
 "建档机构名称" varchar (70) DEFAULT NULL,
 "建档机构联系电话" varchar (20) DEFAULT NULL,
 "建档医生工号" varchar (20) DEFAULT NULL,
 "建档医生姓名" varchar (50) DEFAULT NULL,
 "建档日期" date DEFAULT NULL,
 "档案管理机构代码" varchar (22) DEFAULT NULL,
 "档案管理机构名称" varchar (70) DEFAULT NULL,
 "健康卡发卡机构代码" varchar (22) DEFAULT NULL,
 "健康卡发卡机构名称" varchar (70) DEFAULT NULL,
 "责任医生工号" varchar (20) DEFAULT NULL,
 "责任医生姓名" varchar (50) DEFAULT NULL,
 "责任医生联系电话" varchar (16) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "个人照片" text,
 "工作单位名称" varchar (70) DEFAULT NULL,
 "工作单位联系电话" varchar (20) DEFAULT NULL,
 "本人电话号码" varchar (20) DEFAULT NULL,
 "电子邮件地址" varchar (70) DEFAULT NULL,
 "联系人关系代码" varchar (4) DEFAULT NULL,
 "联系人关系名称" varchar (50) DEFAULT NULL,
 "联系人姓名" varchar (50) DEFAULT NULL,
 "联系人电话号码" varchar (20) DEFAULT NULL,
 "常住地址户籍标志" varchar (1) DEFAULT NULL,
 "常住人口标志" varchar (1) DEFAULT NULL,
 "国籍代码" varchar (10) DEFAULT NULL,
 "国籍名称" varchar (50) DEFAULT NULL,
 "民族代码" varchar (2) DEFAULT NULL,
 "民族名称" varchar (50) DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "ABO血型名称" varchar (50) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "Rh血型名称" varchar (50) DEFAULT NULL,
 "学历代码" varchar (5) DEFAULT NULL,
 "学历名称" varchar (50) DEFAULT NULL,
 CONSTRAINT "(个人)基本信息"_"医疗机构代码"_"个人唯一标识号"_PK PRIMARY KEY ("医疗机构代码",
 "个人唯一标识号")
);


COMMENT ON TABLE "(个人)手术史" IS '个人手术史，包括手术编码及名称、手术机构、手术日期等';
COMMENT ON COLUMN "(个人)手术史"."手术流水号" IS '按照特定编码规则赋予外伤记录的顺序号';
COMMENT ON COLUMN "(个人)手术史"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(个人)手术史"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(个人)手术史"."手术日期" IS '手术及操作完成时的公元纪年日期';
COMMENT ON COLUMN "(个人)手术史"."手术代码" IS '手术及操作在特定编码体系中的唯一标识';
COMMENT ON COLUMN "(个人)手术史"."手术名称" IS '手术及操作在特定编码体系中的名称';
COMMENT ON COLUMN "(个人)手术史"."手术机构代码" IS '手术机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(个人)手术史"."手术机构名称" IS '手术机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个人)手术史"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)手术史"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(个人)手术史"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)手术史"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)手术史"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(个人)手术史"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个人)手术史"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(个人)手术史"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个人)手术史"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(个人)手术史"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个人)手术史"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个人)手术史"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(个人)手术史"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个人)手术史"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(个人)手术史"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
CREATE TABLE IF NOT EXISTS "(
个人)手术史" ("手术流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "手术日期" date DEFAULT NULL,
 "手术代码" varchar (20) DEFAULT NULL,
 "手术名称" varchar (100) DEFAULT NULL,
 "手术机构代码" varchar (22) DEFAULT NULL,
 "手术机构名称" varchar (70) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 CONSTRAINT "(个人)手术史"_"手术流水号"_"医疗机构代码"_PK PRIMARY KEY ("手术流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "(个体)宣教记录" IS '个体宣教情况，包括宣教日期、机构、人员、项目及内容等';
COMMENT ON COLUMN "(个体)宣教记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个体)宣教记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(个体)宣教记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个体)宣教记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(个体)宣教记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个体)宣教记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个体)宣教记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(个体)宣教记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个体)宣教记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(个体)宣教记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个体)宣教记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(个体)宣教记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个体)宣教记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(个体)宣教记录"."宣教流水号" IS '按照某一特性编码规则赋予群体活动记录的顺序号';
COMMENT ON COLUMN "(个体)宣教记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(个体)宣教记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(个体)宣教记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(个体)宣教记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(个体)宣教记录"."宣教日期" IS '完成个体宣教时的公元纪年日期';
COMMENT ON COLUMN "(个体)宣教记录"."宣教机构代码" IS '按照某一特定编码规则赋予宣教机构的唯一标识';
COMMENT ON COLUMN "(个体)宣教记录"."宣教机构名称" IS '宣教机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(个体)宣教记录"."宣教人员工号" IS '宣教人员的工号';
COMMENT ON COLUMN "(个体)宣教记录"."宣教人员姓名" IS '宣教人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(个体)宣教记录"."宣教业务" IS '宣教业务的详细描述';
COMMENT ON COLUMN "(个体)宣教记录"."宣教项目1" IS '宣教项目1的名称';
COMMENT ON COLUMN "(个体)宣教记录"."宣教内容1" IS '医护人员对服务对象进行相关宣传指导活动1的详细描述';
COMMENT ON COLUMN "(个体)宣教记录"."宣教项目2" IS '宣教项目2的名称';
COMMENT ON COLUMN "(个体)宣教记录"."宣教内容2" IS '医护人员对服务对象进行相关宣传指导活动2的详细描述';
COMMENT ON COLUMN "(个体)宣教记录"."宣教项目3" IS '宣教项目3的名称';
COMMENT ON COLUMN "(个体)宣教记录"."宣教内容3" IS '医护人员对服务对象进行相关宣传指导活动3的详细描述';
COMMENT ON COLUMN "(个体)宣教记录"."宣教项目4" IS '宣教项目4的名称';
COMMENT ON COLUMN "(个体)宣教记录"."宣教内容4" IS '医护人员对服务对象进行相关宣传指导活动4的详细描述';
CREATE TABLE IF NOT EXISTS "(
个体)宣教记录" ("登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "宣教流水号" varchar (64) NOT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "宣教日期" date DEFAULT NULL,
 "宣教机构代码" varchar (22) DEFAULT NULL,
 "宣教机构名称" varchar (70) DEFAULT NULL,
 "宣教人员工号" varchar (20) DEFAULT NULL,
 "宣教人员姓名" varchar (50) DEFAULT NULL,
 "宣教业务" varchar (100) DEFAULT NULL,
 "宣教项目1" varchar (200) DEFAULT NULL,
 "宣教内容1" text,
 "宣教项目2" varchar (200) DEFAULT NULL,
 "宣教内容2" text,
 "宣教项目3" varchar (200) DEFAULT NULL,
 "宣教内容3" text,
 "宣教项目4" varchar (200) DEFAULT NULL,
 "宣教内容4" text,
 CONSTRAINT "(个体)宣教记录"_"宣教流水号"_"医疗机构代码"_PK PRIMARY KEY ("宣教流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "(健康体检)住院史" IS '体检住院史子表，包括入院出院日期、住院天数、入院原因、住院机构等';
COMMENT ON COLUMN "(健康体检)住院史"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(健康体检)住院史"."体检流水号" IS '患者以往住院的业务流水号';
COMMENT ON COLUMN "(健康体检)住院史"."住院流水号" IS '按照某一特定编码规则赋予体检就诊记录的顺序号';
COMMENT ON COLUMN "(健康体检)住院史"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(健康体检)住院史"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(健康体检)住院史"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)住院史"."出院时间" IS '患者实际办理出院手续时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)住院史"."住院天数" IS '患者实际的住院天数，入院日与出院日只计算1天';
COMMENT ON COLUMN "(健康体检)住院史"."入院原因" IS '患者入院原因的详细描述';
COMMENT ON COLUMN "(健康体检)住院史"."住院机构代码" IS '患者以往住院的医疗机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(健康体检)住院史"."住院机构名称" IS '患者以往住院的医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)住院史"."住院病案号" IS '按照某一特定编码规则赋予患者在医疗机构住院的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "(健康体检)住院史"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)住院史"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(健康体检)住院史"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)住院史"."登记机构代码" IS '按照某一特定编码规则赋予登记医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)住院史"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)住院史"."修改时间" IS '修改当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)住院史"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(健康体检)住院史"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)住院史"."修改机构代码" IS '按照某一特定编码规则赋予修改医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)住院史"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)住院史"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(健康体检)住院史"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)住院史"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)住院史"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
CREATE TABLE IF NOT EXISTS "(
健康体检)住院史" ("医疗机构名称" varchar (70) DEFAULT NULL,
 "体检流水号" varchar (64) NOT NULL,
 "住院流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "出院时间" timestamp DEFAULT NULL,
 "住院天数" decimal (5,
 0) DEFAULT NULL,
 "入院原因" varchar (100) DEFAULT NULL,
 "住院机构代码" varchar (50) DEFAULT NULL,
 "住院机构名称" varchar (70) DEFAULT NULL,
 "住院病案号" varchar (18) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 CONSTRAINT "(健康体检)住院史"_"体检流水号"_"住院流水号"_"医疗机构代码"_PK PRIMARY KEY ("体检流水号",
 "住院流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "(健康体检)接种记录" IS '体检接种记录子表，包括接种日期、疫苗名称、疫苗批号、接种机构等';
COMMENT ON COLUMN "(健康体检)接种记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)接种记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)接种记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(健康体检)接种记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)接种记录"."修改机构代码" IS '按照某一特定编码规则赋予修改医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)接种记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)接种记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(健康体检)接种记录"."修改时间" IS '修改当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)接种记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)接种记录"."登记机构代码" IS '按照某一特定编码规则赋予登记医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)接种记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)接种记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(健康体检)接种记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)接种记录"."接种机构名称" IS '患者接种疫苗的医疗机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)接种记录"."接种机构代码" IS '患者接种疫苗的医疗机构经《医疗机构执业许可证》登记的，并按照特定编码体系填写的代码';
COMMENT ON COLUMN "(健康体检)接种记录"."疫苗批号" IS '接种疫苗的批号';
COMMENT ON COLUMN "(健康体检)接种记录"."疫苗名称" IS '受检者注射疫苗在特定编码体系中名称';
COMMENT ON COLUMN "(健康体检)接种记录"."疫苗代码" IS '受检者注射疫苗在特定编码体系中的代码';
COMMENT ON COLUMN "(健康体检)接种记录"."接种日期" IS '患者接种疫苗的公元纪年日期';
COMMENT ON COLUMN "(健康体检)接种记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(健康体检)接种记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(健康体检)接种记录"."接种流水号" IS '按照某一特定编码规则赋予体检就诊记录的顺序号';
COMMENT ON COLUMN "(健康体检)接种记录"."体检流水号" IS '按照某一特定编码规则赋予本人某次预防接种的唯一顺序号';
COMMENT ON COLUMN "(健康体检)接种记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(健康体检)接种记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
CREATE TABLE IF NOT EXISTS "(
健康体检)接种记录" ("数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "接种机构名称" varchar (70) DEFAULT NULL,
 "接种机构代码" varchar (22) DEFAULT NULL,
 "疫苗批号" varchar (30) DEFAULT NULL,
 "疫苗名称" varchar (50) DEFAULT NULL,
 "疫苗代码" varchar (2) DEFAULT NULL,
 "接种日期" date DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "接种流水号" varchar (64) NOT NULL,
 "体检流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 CONSTRAINT "(健康体检)接种记录"_"接种流水号"_"体检流水号"_"医疗机构代码"_PK PRIMARY KEY ("接种流水号",
 "体检流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "(健康体检)用药记录" IS '体检用药记录子表，包括药物名称、类别、使用频率、剂量、使用途径等';
COMMENT ON COLUMN "(健康体检)用药记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)用药记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(健康体检)用药记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)用药记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)用药记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(健康体检)用药记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(健康体检)用药记录"."体检流水号" IS '按照某一特定编码规则赋予体检就诊记录的顺序号';
COMMENT ON COLUMN "(健康体检)用药记录"."修改机构代码" IS '按照某一特定编码规则赋予修改医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)用药记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)用药记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(健康体检)用药记录"."修改时间" IS '修改当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)用药记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(健康体检)用药记录"."登记机构代码" IS '按照某一特定编码规则赋予登记医疗机构的唯一标识。这里指登记机构组织机构代码';
COMMENT ON COLUMN "(健康体检)用药记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(健康体检)用药记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(健康体检)用药记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(健康体检)用药记录"."服药依从性名称" IS '患者服药依从性所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(健康体检)用药记录"."服药依从性代码" IS '患者服药依从性所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(健康体检)用药记录"."药物使用途径名称" IS '药物的给药途径在特定编码体系中的名称，如口服、静滴、喷喉等';
COMMENT ON COLUMN "(健康体检)用药记录"."药物使用途径代码" IS '药物使用途径(如口服、静滴、喷喉等)在特定编码体系中的代码';
COMMENT ON COLUMN "(健康体检)用药记录"."药物使用总剂量" IS '在一定时间段内使用药物的总量。根据单次剂量、频次计算得到的总量';
COMMENT ON COLUMN "(健康体检)用药记录"."药物使用次剂量" IS '单次使用药物的剂量，按剂量单位计';
COMMENT ON COLUMN "(健康体检)用药记录"."药物使用剂量单位" IS '药物使用剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "(健康体检)用药记录"."药物使用频率名称" IS '单位时间内药物使用频次类别的标准名称，如每天两次、每周两次、睡前一次等';
COMMENT ON COLUMN "(健康体检)用药记录"."中药类别名称" IS '中药使用类别的标准名称，如未使用、中成药、中草药、其他中药等';
COMMENT ON COLUMN "(健康体检)用药记录"."中药类别代码" IS '中药使用类别(如未使用、中成药、中草药、其他中药)在特定编码体系中的代码';
COMMENT ON COLUMN "(健康体检)用药记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(健康体检)用药记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(健康体检)用药记录"."药物名称" IS '药品在特定编码体系中的名称。对于医疗机构制剂，此处填写医疗机构制剂名称';
CREATE TABLE IF NOT EXISTS "(
健康体检)用药记录" ("修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "体检流水号" varchar (64) NOT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "服药依从性名称" varchar (50) DEFAULT NULL,
 "服药依从性代码" varchar (2) DEFAULT NULL,
 "药物使用途径名称" varchar (50) DEFAULT NULL,
 "药物使用途径代码" varchar (32) DEFAULT NULL,
 "药物使用总剂量" decimal (11,
 2) DEFAULT NULL,
 "药物使用次剂量" decimal (4,
 2) DEFAULT NULL,
 "药物使用剂量单位" varchar (6) DEFAULT NULL,
 "药物使用频率名称" varchar (20) DEFAULT NULL,
 "中药类别名称" varchar (50) DEFAULT NULL,
 "中药类别代码" varchar (2) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "药物名称" varchar (50) NOT NULL,
 CONSTRAINT "(健康体检)用药记录"_"医疗机构代码"_"体检流水号"_"药物名称"_PK PRIMARY KEY ("医疗机构代码",
 "体检流水号",
 "药物名称")
);


COMMENT ON TABLE "(儿童健康体检)体检记录" IS '儿童健康体检记录，包括喂养方式、身高、体重、查体、健康指导等';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."检查流水号" IS '按照一定编码规则赋予体格检查的流水号';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."检查时间" IS '体格检查当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."检查机构代码" IS '按照某一特定编码规则赋予检查医疗机构的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."检查机构名称" IS '检查机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."检查医生工号" IS '检查医师的工号';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."检查医生姓名" IS '检查医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."检查年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."检查年龄(天)" IS '年龄不足1个月的实足年龄的天数';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."下次检查日期" IS '下次体格检查当日的公元纪年日期';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."喂养方式类别代码" IS '婴儿喂养方式的类别(如纯母乳喂养、混合喂养、人工喂养等)在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."喂养方式类别名称" IS '婴儿喂养方式的类别的标准名称，如纯母乳喂养、混合喂养、人工喂养等';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."辅食添加-谷类标志" IS '标识婴儿辅食是否添加谷类食物';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."辅食添加-蛋白类标志" IS '标识婴儿辅食是否添加蛋白类食物';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."辅食添加-水果蔬菜类标志" IS '标识婴儿辅食是否添加水果蔬菜类食物';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."辅食添加-油脂类标志" IS '标识婴儿辅食是否添加油脂类食物';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."辅食添加-其它标志" IS '标识是否添加其他辅食';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."身高(cm)" IS '产妇身高的测量值，计量单位为cm';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."年龄别体重代码" IS '儿童年龄别体重评价结果在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."年龄别体重名称" IS '儿童年龄别体重评价结果在特定编码体系中的名称，如上、中等';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."年龄别身高代码" IS '儿童年龄别身高评价结果在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."年龄别身高名称" IS '儿童年龄别身高评价结果在特定编码体系中的名称，如上、中等';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."身高别体重代码" IS '儿童身高别体重评价结果在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."身高别体重名称" IS '儿童身高别体重评价结果在特定编码体系中的名称，如上、中等';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."体格发育评价代码" IS '儿童体格发育评价结果在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."体格发育评价名称" IS '儿童体格发育评价结果在特定编码体系中的代码，如正常、消瘦、发育迟缓';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."坐高(cm)" IS '儿童坐着时的身高，计量单位为cm';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."头围(cm)" IS '受检者头部周长的测量值，计量单位为cm';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."胸围(cm)" IS '受检者胸部周长的测量值，计量单位为cm';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."皮下脂肪(cm)" IS '皮下脂肪的测量值，计量单位cm';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."面色代码" IS '儿童健康体检时观察儿童面色在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."面色名称" IS '儿童健康体检时观察儿童面色在特定编码体系中的名称，如红润、黄染等';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."面色其他" IS '儿童健康体检时其他儿童面色的描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."皮肤检查异常标志" IS '标识儿童皮肤检查是否异常';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."前囟闭合标志" IS '标识儿童前囟是否已经闭合';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."前囟横径(cm)" IS '新生儿前囟横径的测量值，计量单位为cm';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."前囟纵径(cm)" IS '新生儿前囟纵径的测量值，计量单位为cm';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."颈部包块标志" IS '标识儿童是否有颈部包块';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."眼外观检查异常标志" IS '标识儿童眼外观检查是否异常';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."左眼裸眼远视力值" IS '不借助任何矫正工具，左眼所测得的最佳远视力值';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."右眼裸眼远视力值" IS '不借助任何矫正工具，右眼所测得的最佳远视力值';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."左眼矫正远视力值" IS '借助矫正工具，左眼所测得的最佳远视力值';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."右眼矫正远视力值" IS '借助矫正工具，右眼所测得的最佳远视力值';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."耳外观检查异常标志" IS '标识儿童耳外观检查是否异常';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."听力筛查结果代码" IS '听力筛查的结果在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."听力筛查结果名称" IS '听力筛查的结果在特定编码体系中的名称，如通过、未通过等';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."口腔检查异常标志" IS '标识口腔检查是否异常';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."出牙数(颗)" IS '婴幼儿乳牙萌出的数量,计量单位为颗';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."龋齿数(颗)" IS '个体龋齿的数量 ,计量单位为颗';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."肺部听诊异常标志" IS '标识肺部听诊是否存在异常的标志';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."心脏听诊异常标志" IS '标识心脏听诊是否存在异常标识';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."肝部听诊异常标志" IS '标识肝部听诊是否存在异常';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."脾部听诊异常标志" IS '标识脾部听诊是否存在异常';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."腹部触诊异常标志" IS '标识腹部触诊是否存在异常';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."胸部检查异常标志" IS '标识胸部检查是否存在异常';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."脐带脱落标志" IS '标识脐带是否脱落';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."脐部检查结果代码" IS '对新生儿脐带检查结果在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."脐带检查结果名称" IS '对新生儿脐带检查结果的标准名称';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."脐带检查结果其他" IS '新生儿脐带其他检查结果描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."脐疝标志" IS '标识儿童是否发生脐疝的标志';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."四肢活动度异常标志" IS '标识受检者四肢检查是否异常的标志';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."步态异常标志" IS '标识受检者步态是否异常的标志';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."可疑佝偻病症状代码" IS '对儿童体检时发现可疑佝偻病症状在特定编码体系中的代码。如有多个，以“，”加以分隔';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."可疑佝偻病症状名称" IS '对儿童体检时发现可疑佝偻病症状在特定编码体系中的名称。如有多个，以“，”加以分隔';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."可疑佝偻病症状其他" IS '对儿童体检时发现其他的可疑佝偻病症状描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."可疑佝偻病体征代码" IS '对儿童体检时发现可疑佝偻病体征在特定编码体系中的代码。如有多个，以“，”加以分隔';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."可疑佝偻病体征名称" IS '对儿童体检时发现可疑佝偻病体征在特定编码体系中的名称。如有多个，以“，”加以分隔';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."可疑佝偻病体征其他" IS '对儿童体检时发现其他可疑佝偻病体征的描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."肛门检查异常标志" IS '标识受检者肛门检查是否异常的标志';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."外生殖器检查异常标志" IS '标识受检者外生殖器检查是否异常的标志';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."血红蛋白值(g/L)" IS '受检者单位容积血液中血红蛋白的含量值，计量单位为g/L';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."户外活动时长(h/d)" IS '个体每日在户外活动的平均时长，计量单位为h';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."服用维生素D名称" IS '儿童每日口服维生素D的名称';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."服用维生素D剂量(IU/d)" IS '儿童每日服用维生素D的剂量，计量单位为IU/d';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."发育评估通过标志" IS '标识儿童发育评估是否通过的标志，按照“儿童生长发育监测图”的运动发育指标进行评估每项发育指标根据月龄通过的，为通过，否则为不通过';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."发育评估级联" IS '儿童发育评估级联类别在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."两次随访间患病标志" IS '标识在对儿童进行两次随访之间是否有患病的标志';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."两次随访间患肺炎住院次数" IS '两次随访之间儿童患肺炎主住院的次数';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."两次随访间患腹泻住院次数" IS '两次随访之间儿童因腹泻住院的次数';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."两次随访间因外伤住院次数" IS '两次随访之间儿童因外伤住院的次数';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."两次随访间患其他疾病情况" IS '在对儿童进行两次随访之间所患其他疾病情况的描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."疾病情况" IS '儿童疾病情况描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."浅表淋巴结情况" IS '浅表淋巴结检查结果的详细描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."脊柱情况" IS '脊柱检查结果的详细描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."体征情况" IS '体征检查结果的详细描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."沙眼情况" IS '受检者沙眼衣原体筛查结果描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."鼻检查情况" IS '鼻检查结果的详细描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."咽、扁桃体情况" IS '受检者扁桃体或者咽检查结果描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."萌芽年龄" IS '儿童萌芽年龄描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."肝功能情况" IS '肝功能检查结果的详细描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."DDST筛查/儿心量表结果" IS 'DDST筛查结果/儿心量表结果描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."结论" IS '儿童健康体检检查结果的结论性描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."体弱儿标志" IS '标识儿童是否为体弱儿';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."转诊标志" IS '标识儿童是否需要转诊';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."转诊原因" IS '对转诊原因的简要描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."转入医疗机构名称" IS '转诊转入的医疗卫生机构的组织机构名称';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."转入机构科室名称" IS '转诊转入的医疗机构所属科室在机构内编码体系中的名称';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."健康指导类别代码" IS '对儿童体检后进行健康指导的类别在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."健康指导类别名称" IS '对儿童体检后进行健康指导的类别在特定编码体系中的名称，如科学喂养、合理喂养等';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."健康指导类别其他" IS '对儿童体检后进行健康指导其他类别描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."中医健康管理代码" IS '中医健康管理在特定编码体系中的代码';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."中医健康管理名称" IS '中医健康管理在特定编码体系中的名称，如中医饮食调养指导、捏脊方法、传授摩腹等';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."中医健康管理其他" IS '其他中医健康管理描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(儿童健康体检)体检记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
CREATE TABLE IF NOT EXISTS "(
儿童健康体检)体检记录" ("数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "检查流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "检查时间" timestamp DEFAULT NULL,
 "检查机构代码" varchar (22) DEFAULT NULL,
 "检查机构名称" varchar (70) DEFAULT NULL,
 "检查医生工号" varchar (20) DEFAULT NULL,
 "检查医生姓名" varchar (50) DEFAULT NULL,
 "检查年龄(月)" decimal (2,
 0) DEFAULT NULL,
 "检查年龄(天)" decimal (2,
 0) DEFAULT NULL,
 "下次检查日期" date DEFAULT NULL,
 "喂养方式类别代码" varchar (2) DEFAULT NULL,
 "喂养方式类别名称" varchar (50) DEFAULT NULL,
 "辅食添加-谷类标志" varchar (1) DEFAULT NULL,
 "辅食添加-蛋白类标志" varchar (1) DEFAULT NULL,
 "辅食添加-水果蔬菜类标志" varchar (1) DEFAULT NULL,
 "辅食添加-油脂类标志" varchar (1) DEFAULT NULL,
 "辅食添加-其它标志" varchar (1) DEFAULT NULL,
 "体重(kg)" decimal (4,
 1) DEFAULT NULL,
 "身高(cm)" decimal (4,
 1) DEFAULT NULL,
 "年龄别体重代码" varchar (2) DEFAULT NULL,
 "年龄别体重名称" varchar (50) DEFAULT NULL,
 "年龄别身高代码" varchar (2) DEFAULT NULL,
 "年龄别身高名称" varchar (50) DEFAULT NULL,
 "身高别体重代码" varchar (2) DEFAULT NULL,
 "身高别体重名称" varchar (50) DEFAULT NULL,
 "体格发育评价代码" varchar (2) DEFAULT NULL,
 "体格发育评价名称" varchar (50) DEFAULT NULL,
 "坐高(cm)" decimal (4,
 1) DEFAULT NULL,
 "头围(cm)" decimal (4,
 1) DEFAULT NULL,
 "胸围(cm)" decimal (4,
 1) DEFAULT NULL,
 "皮下脂肪(cm)" decimal (3,
 1) DEFAULT NULL,
 "面色代码" varchar (2) DEFAULT NULL,
 "面色名称" varchar (50) DEFAULT NULL,
 "面色其他" varchar (100) DEFAULT NULL,
 "皮肤检查异常标志" varchar (1) DEFAULT NULL,
 "前囟闭合标志" varchar (1) DEFAULT NULL,
 "前囟横径(cm)" decimal (3,
 1) DEFAULT NULL,
 "前囟纵径(cm)" decimal (3,
 1) DEFAULT NULL,
 "颈部包块标志" varchar (1) DEFAULT NULL,
 "眼外观检查异常标志" varchar (1) DEFAULT NULL,
 "左眼裸眼远视力值" decimal (2,
 1) DEFAULT NULL,
 "右眼裸眼远视力值" decimal (2,
 1) DEFAULT NULL,
 "左眼矫正远视力值" decimal (2,
 1) DEFAULT NULL,
 "右眼矫正远视力值" decimal (2,
 1) DEFAULT NULL,
 "耳外观检查异常标志" varchar (1) DEFAULT NULL,
 "听力筛查结果代码" varchar (1) DEFAULT NULL,
 "听力筛查结果名称" varchar (50) DEFAULT NULL,
 "口腔检查异常标志" varchar (1) DEFAULT NULL,
 "出牙数(颗)" decimal (2,
 0) DEFAULT NULL,
 "龋齿数(颗)" decimal (2,
 0) DEFAULT NULL,
 "肺部听诊异常标志" varchar (1) DEFAULT NULL,
 "心脏听诊异常标志" varchar (1) DEFAULT NULL,
 "肝部听诊异常标志" varchar (1) DEFAULT NULL,
 "脾部听诊异常标志" varchar (1) DEFAULT NULL,
 "腹部触诊异常标志" varchar (1) DEFAULT NULL,
 "胸部检查异常标志" varchar (1) DEFAULT NULL,
 "脐带脱落标志" varchar (1) DEFAULT NULL,
 "脐部检查结果代码" varchar (2) DEFAULT NULL,
 "脐带检查结果名称" varchar (50) DEFAULT NULL,
 "脐带检查结果其他" varchar (100) DEFAULT NULL,
 "脐疝标志" varchar (1) DEFAULT NULL,
 "四肢活动度异常标志" varchar (1) DEFAULT NULL,
 "步态异常标志" varchar (1) DEFAULT NULL,
 "可疑佝偻病症状代码" varchar (30) DEFAULT NULL,
 "可疑佝偻病症状名称" varchar (100) DEFAULT NULL,
 "可疑佝偻病症状其他" varchar (100) DEFAULT NULL,
 "可疑佝偻病体征代码" varchar (30) DEFAULT NULL,
 "可疑佝偻病体征名称" varchar (100) DEFAULT NULL,
 "可疑佝偻病体征其他" varchar (100) DEFAULT NULL,
 "肛门检查异常标志" varchar (1) DEFAULT NULL,
 "外生殖器检查异常标志" varchar (1) DEFAULT NULL,
 "血红蛋白值(g/L)" decimal (3,
 0) DEFAULT NULL,
 "户外活动时长(h/d)" decimal (3,
 1) DEFAULT NULL,
 "服用维生素D名称" varchar (100) DEFAULT NULL,
 "服用维生素D剂量(IU/d)" decimal (5,
 0) DEFAULT NULL,
 "发育评估通过标志" varchar (1) DEFAULT NULL,
 "发育评估级联" varchar (50) DEFAULT NULL,
 "两次随访间患病标志" varchar (1) DEFAULT NULL,
 "两次随访间患肺炎住院次数" decimal (2,
 0) DEFAULT NULL,
 "两次随访间患腹泻住院次数" decimal (2,
 0) DEFAULT NULL,
 "两次随访间因外伤住院次数" decimal (2,
 0) DEFAULT NULL,
 "两次随访间患其他疾病情况" varchar (100) DEFAULT NULL,
 "疾病情况" varchar (100) DEFAULT NULL,
 "浅表淋巴结情况" varchar (100) DEFAULT NULL,
 "脊柱情况" varchar (100) DEFAULT NULL,
 "体征情况" varchar (100) DEFAULT NULL,
 "沙眼情况" varchar (100) DEFAULT NULL,
 "鼻检查情况" varchar (100) DEFAULT NULL,
 "咽、扁桃体情况" varchar (100) DEFAULT NULL,
 "萌芽年龄" varchar (100) DEFAULT NULL,
 "肝功能情况" varchar (100) DEFAULT NULL,
 "DDST筛查/儿心量表结果" varchar (100) DEFAULT NULL,
 "结论" varchar (100) DEFAULT NULL,
 "体弱儿标志" varchar (1) DEFAULT NULL,
 "转诊标志" varchar (1) DEFAULT NULL,
 "转诊原因" varchar (100) DEFAULT NULL,
 "转入医疗机构名称" varchar (70) DEFAULT NULL,
 "转入机构科室名称" varchar (100) DEFAULT NULL,
 "健康指导类别代码" varchar (30) DEFAULT NULL,
 "健康指导类别名称" varchar (100) DEFAULT NULL,
 "健康指导类别其他" varchar (100) DEFAULT NULL,
 "中医健康管理代码" varchar (30) DEFAULT NULL,
 "中医健康管理名称" varchar (50) DEFAULT NULL,
 "中医健康管理其他" varchar (100) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 CONSTRAINT "(儿童健康体检)体检记录"_"医疗机构代码"_"检查流水号"_PK PRIMARY KEY ("医疗机构代码",
 "检查流水号")
);


COMMENT ON TABLE "(儿童健康体检)检验记录" IS '儿童健康体检检验记录，包括血红蛋白、白细胞、红细胞、乙肝等具体指标检验结果';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."网织红细胞(%)" IS '网织红细胞占比';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."乙肝表面抗原检测阳性标志" IS '标识乙肝表面抗原检测是否为阳性';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."乙肝表面抗原检测机构代码" IS '乙肝表面抗原检测机构的组织机构代码';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."乙肝表面抗原检测机构名称" IS '乙肝表面抗原检测机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."骨碱性磷酸酶" IS '骨骼中骨型碱性磷酸酶的含量，计量单位u/l';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."骨密度" IS '骨密度检查结果描述';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."X片检查" IS 'X片检查结果描述';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."其他检查结果" IS '除上述内容外，其他检查结果的详细描述';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."检查流水号" IS '按照某一特定编码规则赋予本人某次预防接种的唯一顺序号';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."检查时间" IS '检查完成当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."检查机构代码" IS '按照某一特定编码规则赋予检查医疗机构的唯一标识';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."检查机构名称" IS '检查机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."检查医生工号" IS '检查医师的工号';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."检查医生姓名" IS '检查医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."检查年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."检查年龄(天)" IS '年龄不足1个月的实足年龄的天数';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."血液检查检志" IS '标识儿童是否进行血液类检查';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."血红蛋白值(g/L)" IS '受检者单位容积血液中血红蛋白的含量值，计量单位为g/L';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."红细胞计数值" IS '受检者单位容积血液内红细胞的数量值，计量单位为10^12/L';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."白细胞计数值(G/L)" IS '受检者单位容积血液中白细胞数量值，计量单位为10^9/L';
COMMENT ON COLUMN "(儿童健康体检)检验记录"."血清铁蛋白" IS '血清铁蛋白检测值';
CREATE TABLE IF NOT EXISTS "(
儿童健康体检)检验记录" ("网织红细胞(%)" decimal (3,
 2) DEFAULT NULL,
 "乙肝表面抗原检测阳性标志" varchar (1) DEFAULT NULL,
 "乙肝表面抗原检测机构代码" varchar (64) DEFAULT NULL,
 "乙肝表面抗原检测机构名称" varchar (100) DEFAULT NULL,
 "骨碱性磷酸酶" decimal (3,
 0) DEFAULT NULL,
 "骨密度" decimal (2,
 1) DEFAULT NULL,
 "X片检查" varchar (100) DEFAULT NULL,
 "其他检查结果" varchar (100) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "检查流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "检查时间" timestamp DEFAULT NULL,
 "检查机构代码" varchar (22) DEFAULT NULL,
 "检查机构名称" varchar (70) DEFAULT NULL,
 "检查医生工号" varchar (20) DEFAULT NULL,
 "检查医生姓名" varchar (50) DEFAULT NULL,
 "检查年龄(月)" decimal (2,
 0) DEFAULT NULL,
 "检查年龄(天)" decimal (2,
 0) DEFAULT NULL,
 "血液检查检志" varchar (1) DEFAULT NULL,
 "血红蛋白值(g/L)" decimal (3,
 0) DEFAULT NULL,
 "红细胞计数值" decimal (3,
 1) DEFAULT NULL,
 "白细胞计数值(G/L)" decimal (3,
 1) DEFAULT NULL,
 "血清铁蛋白" decimal (3,
 0) DEFAULT NULL,
 CONSTRAINT "(儿童健康体检)检验记录"_"医疗机构代码"_"检查流水号"_PK PRIMARY KEY ("医疗机构代码",
 "检查流水号")
);


COMMENT ON TABLE "(冠心病)年度评估报告" IS '冠心病患者年度评估，包括危险因素控制效果、非药物治疗效果、病情转归、评估结果、指导建议等';
COMMENT ON COLUMN "(冠心病)年度评估报告"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(冠心病)年度评估报告"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(冠心病)年度评估报告"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(冠心病)年度评估报告"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(冠心病)年度评估报告"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(冠心病)年度评估报告"."评估机构名称" IS '评估机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(冠心病)年度评估报告"."评估机构编号" IS '评估医疗机构的组织机构代码';
COMMENT ON COLUMN "(冠心病)年度评估报告"."评估医生姓名" IS '评估医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(冠心病)年度评估报告"."评估医生工号" IS '评估医师的工号';
COMMENT ON COLUMN "(冠心病)年度评估报告"."评估日期" IS '对患者进行评估当日的公元纪年日期';
COMMENT ON COLUMN "(冠心病)年度评估报告"."指导建议" IS '描述医师针对病人情况而提出的指导建议';
COMMENT ON COLUMN "(冠心病)年度评估报告"."年度评估结果" IS '年度评估结果的详细描述';
COMMENT ON COLUMN "(冠心病)年度评估报告"."异常详述" IS '对异常情况的详细描述';
COMMENT ON COLUMN "(冠心病)年度评估报告"."危险级别转归" IS '危险级别转归的描述';
COMMENT ON COLUMN "(冠心病)年度评估报告"."病情转归代码" IS '疾病治疗结果的类别(如治愈、好转、稳定、恶化等)在特定编码体系中的代码';
COMMENT ON COLUMN "(冠心病)年度评估报告"."非药物治疗效果" IS '非药物治疗效果的详细描述';
COMMENT ON COLUMN "(冠心病)年度评估报告"."药物治疗效果" IS '药物治疗效果的详细描述';
COMMENT ON COLUMN "(冠心病)年度评估报告"."危险因素控制效果" IS '危险因素控制效果的详细描述';
COMMENT ON COLUMN "(冠心病)年度评估报告"."专项档案标识号" IS '按照某一特定规则赋予专项档案的唯一标识';
COMMENT ON COLUMN "(冠心病)年度评估报告"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(冠心病)年度评估报告"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(冠心病)年度评估报告"."评估流水号" IS '按照一定编码规则赋予产后访视记录的顺序号';
COMMENT ON COLUMN "(冠心病)年度评估报告"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(冠心病)年度评估报告"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(冠心病)年度评估报告"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(冠心病)年度评估报告"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(冠心病)年度评估报告"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(冠心病)年度评估报告"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(冠心病)年度评估报告"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(冠心病)年度评估报告"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(冠心病)年度评估报告"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(冠心病)年度评估报告"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
CREATE TABLE IF NOT EXISTS "(
冠心病)年度评估报告" ("数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "评估机构名称" varchar (70) DEFAULT NULL,
 "评估机构编号" varchar (22) DEFAULT NULL,
 "评估医生姓名" varchar (50) DEFAULT NULL,
 "评估医生工号" varchar (20) DEFAULT NULL,
 "评估日期" date DEFAULT NULL,
 "指导建议" varchar (500) DEFAULT NULL,
 "年度评估结果" varchar (500) DEFAULT NULL,
 "异常详述" varchar (500) DEFAULT NULL,
 "危险级别转归" varchar (10) DEFAULT NULL,
 "病情转归代码" varchar (10) DEFAULT NULL,
 "非药物治疗效果" varchar (10) DEFAULT NULL,
 "药物治疗效果" varchar (10) DEFAULT NULL,
 "危险因素控制效果" varchar (10) DEFAULT NULL,
 "专项档案标识号" varchar (64) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "评估流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "(冠心病)年度评估报告"_"评估流水号"_"医疗机构代码"_PK PRIMARY KEY ("评估流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "(孕产妇)产前诊断" IS '孕妇产前诊断信息';
COMMENT ON COLUMN "(孕产妇)产前诊断"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)产前诊断"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产前诊断"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产前诊断"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(孕产妇)产前诊断"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)产前诊断"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)产前诊断"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产前诊断"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(孕产妇)产前诊断"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产前诊断"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产前诊断"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(孕产妇)产前诊断"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产前诊断"."诊断流水号" IS '按照某一特性编码规则赋予筛查记录的顺序号';
COMMENT ON COLUMN "(孕产妇)产前诊断"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(孕产妇)产前诊断"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(孕产妇)产前诊断"."孕册编号" IS '按照某一特定编码规则赋予孕产妇孕册的唯一标识';
COMMENT ON COLUMN "(孕产妇)产前诊断"."诊断时间" IS '对患者进行产前诊断时当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "(孕产妇)产前诊断"."诊断机构代码" IS '诊断机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)产前诊断"."诊断机构名称" IS '诊断机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产前诊断"."诊断医生工号" IS '诊断医生的工号';
COMMENT ON COLUMN "(孕产妇)产前诊断"."诊断医生姓名" IS '诊断医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)产前诊断"."诊断孕周(周)" IS '产前诊断下达时孕妇的妊娠时长，计量单位为周';
COMMENT ON COLUMN "(孕产妇)产前诊断"."诊断孕周(天)" IS '产前诊断下达孕妇超出上述孕周的天数，计量单位为天';
COMMENT ON COLUMN "(孕产妇)产前诊断"."诊断项目" IS '对孕妇进行产前诊断时所涉及的检查项目描述';
COMMENT ON COLUMN "(孕产妇)产前诊断"."诊断方法" IS '对孕妇进行产前诊断时所采用方法的描述';
COMMENT ON COLUMN "(孕产妇)产前诊断"."诊断结果" IS '对孕妇进行产前诊断后结果的结论性描述';
COMMENT ON COLUMN "(孕产妇)产前诊断"."诊断医学意见" IS '对孕妇进行产前诊断后给予医学指导意见的详细描述';
COMMENT ON COLUMN "(孕产妇)产前诊断"."妊娠结局" IS '孕妇妊娠最终结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产前诊断"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产前诊断"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(孕产妇)产前诊断"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
CREATE TABLE IF NOT EXISTS "(
孕产妇)产前诊断" ("登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "诊断流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "孕册编号" varchar (64) DEFAULT NULL,
 "诊断时间" timestamp DEFAULT NULL,
 "诊断机构代码" varchar (22) DEFAULT NULL,
 "诊断机构名称" varchar (70) DEFAULT NULL,
 "诊断医生工号" varchar (20) DEFAULT NULL,
 "诊断医生姓名" varchar (50) DEFAULT NULL,
 "诊断孕周(周)" decimal (2,
 0) DEFAULT NULL,
 "诊断孕周(天)" decimal (1,
 0) DEFAULT NULL,
 "诊断项目" varchar (100) DEFAULT NULL,
 "诊断方法" varchar (100) DEFAULT NULL,
 "诊断结果" text,
 "诊断医学意见" varchar (100) DEFAULT NULL,
 "妊娠结局" varchar (50) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 CONSTRAINT "(孕产妇)产前诊断"_"医疗机构代码"_"诊断流水号"_PK PRIMARY KEY ("医疗机构代码",
 "诊断流水号")
);


COMMENT ON TABLE "(孕产妇)产后新生儿访视" IS '产后新生儿访视信息，包括身长、体重、喂养、大便、睡眠、查体等';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."心脏听诊异常标志" IS '标识心脏听诊是否存在异常标识';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."胸部检查异常标志" IS '标识胸部检查是否存在异常标识';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."胸部检查异常结果描述" IS '胸部检查异常结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."腹部触诊异常标志" IS '标识腹部触诊是否存在异常标识';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."腹部触诊异常结果描述" IS '腹部触诊异常结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."四肢活动度异常标志" IS '标识受检者四肢检查是否异常的标志';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."四肢活动度异常结果描述" IS '四肢活动度异常结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."颈部包块标志" IS '标识受检者颈部检查是否异常的标志';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."下次访视地点" IS '对新生儿进行下次随访的地点描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."下次随访日期" IS '对新生儿进行下次随访的公元纪年日期';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."健康指导类别其他" IS '除上述内容外，其他儿童健康指导类别的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."健康指导类别名称" IS '对新生儿家庭访视后进行健康指导的类别在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."健康指导类别代码" IS '对新生儿家庭访视后进行健康指导的类别在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."转入机构科室名称" IS '转入科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."转入医疗机构名称" IS '转入医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."转诊原因" IS '对孕产妇转诊原因的简要描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."转诊标志" IS '标识孕产妇是否转诊的标志';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."脐带检查结果其他" IS '除上述类别外，其他脐带检查结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."脐带检查结果名称" IS '对新生儿脐带检查结果的标准名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."脐部检查结果代码" IS '对新生儿脐带检查结果在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."脊柱检查异常结果描述" IS '脊柱检查异常结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."脊柱检查异常标志" IS '标识受检者脊柱检查是否异常的标志';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."外生殖器检查异常结果描述" IS '外生殖器检查异常结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."外生殖器检查异常标志" IS '标识受检者外生殖器检查是否异常的标志';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."肛门检查异常结果描述" IS '肛门检查异常结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."肛门检查异常标志" IS '标识受检者肛门检查是否异常的标志';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."皮肤检查结果其他" IS '皮肤检查结果的其他的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."皮肤检查结果名称" IS '皮肤检查结果在特定编码体系中的代名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."皮肤检查结果代码" IS '皮肤检查结果在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."颈部包块检查结果描述" IS '颈部包块检查结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."心脏检查异常结果描述" IS '心脏检查异常结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."随访流水号" IS '按照某一特定编码规则赋予随访记录的顺序号';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."孕册编号" IS '按照某一特定编码规则赋予孕产妇孕册的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."新生儿流水号" IS '按照某一特定编码规则赋予新生儿信息记录的顺序号';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."身份证号" IS '个体居民身份证上的唯一法定标识符';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."家庭地址" IS '家庭地址的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."母亲保健登记编号" IS '按照一定编码规则赋予母亲保健登记记录的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."母亲妊娠期患病情况代码" IS '母亲妊娠期患病情况的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."母亲妊娠期患病其他详述" IS '母亲妊娠期患病其他情况的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."出生情况代码" IS '新生儿出生情况的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."出生情况其他详述" IS '新生儿其他出生情况的描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."助产机构代码" IS '按照某一特定编码规则赋予助产机构的唯一标识。这里指医疗机构组织机构代码';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."助产机构名称" IS '助产机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."随访日期" IS '对患者进行随访时当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."随访医生工号" IS '随访医师的工号';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."随访医生姓名" IS '随访医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."随访机构代码" IS '随访机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."随访机构名称" IS '随访机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."实足天数" IS '新生儿自出生至当前的实足天数';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."体重(g)" IS '体重的测量值，计量单位为g';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."身长(cm)" IS '儿童卧位身长的测量值，计量单位为cm';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."喂养方式类别代码" IS '婴儿喂养方式的类别(如纯母乳喂养、混合喂养、人工喂养等)在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."喂养方式类别名称" IS '婴儿喂养方式的类别的标准名称，如纯母乳喂养、混合喂养、人工喂养等';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."吃奶次数(次/d)" IS '儿童每天吃奶的次数，计量单位为次';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."每次吃奶量(mL/次)" IS '儿童每次吃奶的量，计量单位为mL';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."每天吃奶量(mL/d)" IS '儿童每天吃奶的量，计量单位为mL';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."呕吐标志" IS '标识对患者是有呕吐症状的标志';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."大便性状代码" IS '大便的性状的类型在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."大便性状名称" IS '大便的性状的类型在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."大便性状其他" IS '除上述内容外，其他大便性状类别的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."大便次数(次/日)" IS '每天大便的次数，计数单位次/d';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."脉率(次/min)" IS '每分钟脉搏的次数测量值，计量单位为次/min';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."呼吸频率(次/min)" IS '单位时间内呼吸的次数,计壁单位为次/min';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."睡眠质量代码" IS '新生儿睡眠的质量状况所属类型(如好、一般、差、不详)在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."睡眠质量名称" IS '新生儿睡眠的质量状况所属类型的标准名称，如好、一般、差、不详';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."睡眠情况代码" IS '新生儿在睡眠时的身体情况(如安静、汗多、烦躁、夜惊)在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."睡眠情况名称" IS '新生儿在睡眠时的身体情况的标准名称，如安静、汗多、烦躁、夜惊';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."面色代码" IS '新生儿健康体检时观察新生儿面色在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."面色名称" IS '新生儿健康体检时观察新生儿面色在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."面色其他" IS '除上述内容外，其他面色类别的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."黄疸部位代码" IS '个体出现皮肤黄疸体征的具体部位在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."黄疸部位名称" IS '个体出现皮肤黄疸体征的具体部位在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."黄疸部位其他" IS '除上述类别外，其他黄疸部位的名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."前囟横径(cm)" IS '新生儿前囟横径的测量值，计量单位为cm';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."前囟纵径(cm)" IS '新生儿前囟纵径的测量值，计量单位为cm';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."前囟张力代码" IS '婴儿体检时发现前囟张力大小在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."前囟张力名称" IS '婴儿体检时发现前囟张力大小的标准名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."前囟张力其他" IS '除上述类别外，其他前囟张力的名称';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."眼外观检查异常标志" IS '标识个体眼外观检查是否异常的标志';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."眼外观检查异常结果描述" IS '眼外观检查异常结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."耳外观检查异常标志" IS '标识受检者耳外观检查是否异常的标志';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."耳外观检查异常结果描述" IS '对患者耳外观检查异常结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."鼻检查异常标志" IS '标识受检者鼻部检查是否异常的标志';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."鼻检查异常结果描述" IS '鼻部检查结果的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."口腔检查异常标志" IS '标识受检者口腔检查是否异常的标志';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."口腔检查异常结果描述" IS '受检者口腔检查异常情况的详细描述';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."肺部听诊异常标志" IS '标识肺部听诊是否存在异常的标志';
COMMENT ON COLUMN "(孕产妇)产后新生儿访视"."肺部检查异常结果描述" IS '肺部检查异常结果的详细描述';
CREATE TABLE IF NOT EXISTS "(
孕产妇)产后新生儿访视" ("心脏听诊异常标志" varchar (1) DEFAULT NULL,
 "胸部检查异常标志" varchar (1) DEFAULT NULL,
 "胸部检查异常结果描述" varchar (100) DEFAULT NULL,
 "腹部触诊异常标志" varchar (1) DEFAULT NULL,
 "腹部触诊异常结果描述" varchar (100) DEFAULT NULL,
 "四肢活动度异常标志" varchar (1) DEFAULT NULL,
 "四肢活动度异常结果描述" varchar (10) DEFAULT NULL,
 "颈部包块标志" varchar (1) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "下次访视地点" varchar (100) DEFAULT NULL,
 "下次随访日期" date DEFAULT NULL,
 "健康指导类别其他" varchar (100) DEFAULT NULL,
 "健康指导类别名称" varchar (100) DEFAULT NULL,
 "健康指导类别代码" varchar (30) DEFAULT NULL,
 "转入机构科室名称" varchar (100) DEFAULT NULL,
 "转入医疗机构名称" varchar (70) DEFAULT NULL,
 "转诊原因" varchar (100) DEFAULT NULL,
 "转诊标志" varchar (1) DEFAULT NULL,
 "脐带检查结果其他" varchar (100) DEFAULT NULL,
 "脐带检查结果名称" varchar (50) DEFAULT NULL,
 "脐部检查结果代码" varchar (2) DEFAULT NULL,
 "脊柱检查异常结果描述" varchar (100) DEFAULT NULL,
 "脊柱检查异常标志" varchar (1) DEFAULT NULL,
 "外生殖器检查异常结果描述" varchar (100) DEFAULT NULL,
 "外生殖器检查异常标志" varchar (1) DEFAULT NULL,
 "肛门检查异常结果描述" varchar (100) DEFAULT NULL,
 "肛门检查异常标志" varchar (1) DEFAULT NULL,
 "皮肤检查结果其他" varchar (100) DEFAULT NULL,
 "皮肤检查结果名称" varchar (50) DEFAULT NULL,
 "皮肤检查结果代码" varchar (2) DEFAULT NULL,
 "颈部包块检查结果描述" varchar (100) DEFAULT NULL,
 "心脏检查异常结果描述" varchar (100) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "随访流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "孕册编号" varchar (64) NOT NULL,
 "新生儿流水号" varchar (64) DEFAULT NULL,
 "身份证号" varchar (18) DEFAULT NULL,
 "家庭地址" varchar (200) DEFAULT NULL,
 "母亲保健登记编号" varchar (20) DEFAULT NULL,
 "母亲妊娠期患病情况代码" varchar (30) DEFAULT NULL,
 "母亲妊娠期患病其他详述" varchar (200) DEFAULT NULL,
 "出生情况代码" varchar (30) DEFAULT NULL,
 "出生情况其他详述" varchar (200) DEFAULT NULL,
 "助产机构代码" varchar (50) DEFAULT NULL,
 "助产机构名称" varchar (200) DEFAULT NULL,
 "随访日期" date DEFAULT NULL,
 "随访医生工号" varchar (20) DEFAULT NULL,
 "随访医生姓名" varchar (50) DEFAULT NULL,
 "随访机构代码" varchar (22) DEFAULT NULL,
 "随访机构名称" varchar (70) DEFAULT NULL,
 "实足天数" decimal (2,
 0) DEFAULT NULL,
 "体重(g)" decimal (4,
 0) DEFAULT NULL,
 "身长(cm)" decimal (4,
 1) DEFAULT NULL,
 "喂养方式类别代码" varchar (2) DEFAULT NULL,
 "喂养方式类别名称" varchar (50) DEFAULT NULL,
 "吃奶次数(次/d)" decimal (2,
 0) DEFAULT NULL,
 "每次吃奶量(mL/次)" decimal (4,
 0) DEFAULT NULL,
 "每天吃奶量(mL/d)" decimal (4,
 0) DEFAULT NULL,
 "呕吐标志" varchar (1) DEFAULT NULL,
 "大便性状代码" varchar (2) DEFAULT NULL,
 "大便性状名称" varchar (50) DEFAULT NULL,
 "大便性状其他" varchar (100) DEFAULT NULL,
 "大便次数(次/日)" decimal (2,
 0) DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "脉率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "呼吸频率(次/min)" decimal (3,
 0) DEFAULT NULL,
 "睡眠质量代码" varchar (1) DEFAULT NULL,
 "睡眠质量名称" varchar (50) DEFAULT NULL,
 "睡眠情况代码" varchar (2) DEFAULT NULL,
 "睡眠情况名称" varchar (50) DEFAULT NULL,
 "面色代码" varchar (2) DEFAULT NULL,
 "面色名称" varchar (50) DEFAULT NULL,
 "面色其他" varchar (100) DEFAULT NULL,
 "黄疸部位代码" varchar (30) DEFAULT NULL,
 "黄疸部位名称" varchar (100) DEFAULT NULL,
 "黄疸部位其他" varchar (100) DEFAULT NULL,
 "前囟横径(cm)" decimal (3,
 1) DEFAULT NULL,
 "前囟纵径(cm)" decimal (3,
 1) DEFAULT NULL,
 "前囟张力代码" varchar (2) DEFAULT NULL,
 "前囟张力名称" varchar (50) DEFAULT NULL,
 "前囟张力其他" varchar (100) DEFAULT NULL,
 "眼外观检查异常标志" varchar (1) DEFAULT NULL,
 "眼外观检查异常结果描述" varchar (100) DEFAULT NULL,
 "耳外观检查异常标志" varchar (1) DEFAULT NULL,
 "耳外观检查异常结果描述" varchar (100) DEFAULT NULL,
 "鼻检查异常标志" varchar (1) DEFAULT NULL,
 "鼻检查异常结果描述" varchar (100) DEFAULT NULL,
 "口腔检查异常标志" varchar (1) DEFAULT NULL,
 "口腔检查异常结果描述" varchar (100) DEFAULT NULL,
 "肺部听诊异常标志" varchar (1) DEFAULT NULL,
 "肺部检查异常结果描述" varchar (100) DEFAULT NULL,
 CONSTRAINT "(孕产妇)产后新生儿访视"_"医疗机构代码"_"随访流水号"_"孕册编号"_PK PRIMARY KEY ("医疗机构代码",
 "随访流水号",
 "孕册编号")
);


COMMENT ON TABLE "(孕产妇)第2-5次产前随访服务记录" IS '孕妇第2-5次产前检查结果信息';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."随访流水号" IS '按照某一特定编码规则赋予筛查记录的顺序号';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."孕册编号" IS '按照某一特定编码规则赋予孕产妇孕册的唯一标识';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."随访日期" IS '对患者进行随访时当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."随访医生工号" IS '随访医师的工号';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."随访医生姓名" IS '随访医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."随访机构代码" IS '随访机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."随访机构名称" IS '随访机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."随访孕周(周)" IS '随访发生时孕妇的妊娠时长，计量单位为周';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."随访孕周(天)" IS '随访发生时孕妇超出上述孕周的天数，计量单位为天';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."主诉" IS '患者向医师描述的对自身本次疾病相关的感受的主要记录';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."宫底高度(cm)" IS '宫底高度的测量值，计量单位为cm';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."腹围(cm)" IS '受检者腹部周长的测量值，计量单位为cm';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."胎方位代码" IS '胎儿方位的类别在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."胎方位名称" IS '胎儿方位的类别在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."胎心率(次/min)" IS '单位时间内胎儿胎心搏动的次数，计量单位为次/min';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."血红蛋白值(g/L)" IS '受检者单位容积血液中血红蛋白的含量值，计量单位为g/L';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."尿蛋白定量检测值(mg/24h)" IS '采用定量检测方法测得的24h尿蛋白含量，计量单位为mg/24h';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."尿蛋白定性检测结果代码" IS '尿蛋白定性检测结果在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."尿蛋白定性检测结果名称" IS '尿蛋白定性检测结果在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."其他辅助检查结果" IS '其他辅助检查结果的详细描述';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."健康评估异常标志" IS '标识孕产妇健康评估结论是否异常的标志';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."健康评估异常结果描述" IS '孕产妇健康评估异常结果的详细描述';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."健康保健指导代码" IS '对孕产妇进行健康指导类别在特定编码体系中的代码';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."健康保健指导名称" IS '对孕产妇进行健康指导类别在特定编码体系中的名称';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."健康保健指导其他" IS '除上述健康保健指导外，其他孕产妇健康指导类别名称';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."转诊标志" IS '标识孕产妇是否转诊的标志';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."转诊原因" IS '对孕产妇转诊原因的简要描述';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."转入医疗机构名称" IS '转入医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."转入机构科室名称" IS '转入科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."下次随访日期" IS '对孕产妇进行下次随访的公元纪年日期';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)第2-5次产前随访服务记录"."登记人员工号" IS '登记人员的工号';
CREATE TABLE IF NOT EXISTS "(
孕产妇)第2-5次产前随访服务记录" ("登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "随访流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "孕册编号" varchar (64) DEFAULT NULL,
 "随访日期" date DEFAULT NULL,
 "随访医生工号" varchar (20) DEFAULT NULL,
 "随访医生姓名" varchar (50) DEFAULT NULL,
 "随访机构代码" varchar (22) DEFAULT NULL,
 "随访机构名称" varchar (70) DEFAULT NULL,
 "随访孕周(周)" decimal (2,
 0) DEFAULT NULL,
 "随访孕周(天)" decimal (1,
 0) DEFAULT NULL,
 "主诉" text,
 "体重(kg)" decimal (4,
 1) DEFAULT NULL,
 "宫底高度(cm)" decimal (3,
 1) DEFAULT NULL,
 "腹围(cm)" decimal (4,
 1) DEFAULT NULL,
 "胎方位代码" varchar (30) DEFAULT NULL,
 "胎方位名称" varchar (100) DEFAULT NULL,
 "胎心率(次/min)" decimal (3,
 0) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "血红蛋白值(g/L)" decimal (3,
 0) DEFAULT NULL,
 "尿蛋白定量检测值(mg/24h)" decimal (4,
 1) DEFAULT NULL,
 "尿蛋白定性检测结果代码" varchar (2) DEFAULT NULL,
 "尿蛋白定性检测结果名称" varchar (50) DEFAULT NULL,
 "其他辅助检查结果" varchar (100) DEFAULT NULL,
 "健康评估异常标志" varchar (1) DEFAULT NULL,
 "健康评估异常结果描述" varchar (100) DEFAULT NULL,
 "健康保健指导代码" varchar (30) DEFAULT NULL,
 "健康保健指导名称" varchar (100) DEFAULT NULL,
 "健康保健指导其他" varchar (100) DEFAULT NULL,
 "转诊标志" varchar (1) DEFAULT NULL,
 "转诊原因" varchar (100) DEFAULT NULL,
 "转入医疗机构名称" varchar (70) DEFAULT NULL,
 "转入机构科室名称" varchar (100) DEFAULT NULL,
 "下次随访日期" date DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 CONSTRAINT "(孕产妇)第2-5次产前随访服务记录"_"医疗机构代码"_"随访流水号"_PK PRIMARY KEY ("医疗机构代码",
 "随访流水号")
);


COMMENT ON TABLE "(孕产妇)高危随诊" IS '孕妇高危随诊信息，包括随诊孕周、症状体征、复制检查结果、处理指导意见等';
COMMENT ON COLUMN "(孕产妇)高危随诊"."随诊孕周(周)" IS '随诊发生时孕妇的妊娠时长，计量单位为周';
COMMENT ON COLUMN "(孕产妇)高危随诊"."随诊孕周(天)" IS '随诊发生时孕妇超出上述孕周的天数，计量单位为天';
COMMENT ON COLUMN "(孕产妇)高危随诊"."症状描述" IS '对高危孕产妇出现症状的详细描述';
COMMENT ON COLUMN "(孕产妇)高危随诊"."体征描述" IS '对高危孕产妇出现阳性体征的详细描述';
COMMENT ON COLUMN "(孕产妇)高危随诊"."辅助检查结果" IS '患者辅助检查结果的详细描述';
COMMENT ON COLUMN "(孕产妇)高危随诊"."处理指导意见" IS '对高危孕产妇进行处理及指导意见内容的详细描述';
COMMENT ON COLUMN "(孕产妇)高危随诊"."下次随诊日期" IS '对孕产妇进行下次随诊的公元纪年日期';
COMMENT ON COLUMN "(孕产妇)高危随诊"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)高危随诊"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(孕产妇)高危随诊"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)高危随诊"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)高危随诊"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)高危随诊"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)高危随诊"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(孕产妇)高危随诊"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(孕产妇)高危随诊"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)高危随诊"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)高危随诊"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(孕产妇)高危随诊"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)高危随诊"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(孕产妇)高危随诊"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(孕产妇)高危随诊"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)高危随诊"."随诊流水号" IS '按照某一特定编码规则赋予新生儿信息记录的顺序号';
COMMENT ON COLUMN "(孕产妇)高危随诊"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(孕产妇)高危随诊"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(孕产妇)高危随诊"."孕册编号" IS '按照某一特定编码规则赋予孕产妇孕册的唯一标识';
COMMENT ON COLUMN "(孕产妇)高危随诊"."随诊日期" IS '对患者进行随诊时当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "(孕产妇)高危随诊"."随诊机构代码" IS '随诊机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(孕产妇)高危随诊"."随诊机构名称" IS '随诊机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(孕产妇)高危随诊"."随诊医生工号" IS '随诊医师的工号';
COMMENT ON COLUMN "(孕产妇)高危随诊"."随诊医生姓名" IS '随诊医师在公安户籍管理部门正式登记注册的姓氏和名称';
CREATE TABLE IF NOT EXISTS "(
孕产妇)高危随诊" ("随诊孕周(周)" decimal (2,
 0) DEFAULT NULL,
 "随诊孕周(天)" decimal (1,
 0) DEFAULT NULL,
 "症状描述" varchar (1000) DEFAULT NULL,
 "体征描述" varchar (100) DEFAULT NULL,
 "辅助检查结果" varchar (1000) DEFAULT NULL,
 "处理指导意见" varchar (200) DEFAULT NULL,
 "下次随诊日期" date DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "随诊流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "孕册编号" varchar (64) DEFAULT NULL,
 "随诊日期" date DEFAULT NULL,
 "随诊机构代码" varchar (22) DEFAULT NULL,
 "随诊机构名称" varchar (70) DEFAULT NULL,
 "随诊医生工号" varchar (20) DEFAULT NULL,
 "随诊医生姓名" varchar (50) DEFAULT NULL,
 CONSTRAINT "(孕产妇)高危随诊"_"医疗机构代码"_"随诊流水号"_PK PRIMARY KEY ("医疗机构代码",
 "随诊流水号")
);


COMMENT ON TABLE "(新冠肺炎)专项档案" IS '新冠肺炎专项档案，包括个人基本信息、管理所在行政区划、最新人员状态、最新处置措施、旅居及接触史等';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."疑似收治日期" IS '患者作为疑似病例收治时的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."疑似收治机构代码" IS '按照某一特定编码规则赋予疑似-收治机构的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."疑似收治机构名称" IS '疑似-收治机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."确诊收治日期" IS '患者作为确诊病例收治时的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."确诊收治机构代码" IS '按照某一特定编码规则赋予确诊收治机构的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."确诊收治机构名称" IS '确诊-收治机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."专项档案标识号" IS '按照某一特定规则赋予专项档案的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."调查表流水号" IS '按照某一特定编码规则赋予调查表的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."姓名" IS '本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."家属姓名" IS '家属在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."联系电话号码" IS '患者本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."户籍地址" IS '户籍地址的详细描述';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."户籍地址行政区划代码" IS '户籍地址所在区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."居住地址" IS '现住地址的详细描述';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."居住地-行政区划代码" IS '居住地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."管理所在行政区划代码" IS '管理所在行政区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."管理所在行政区划名称" IS '管理所在行政区划的详细描述';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."建档时人员状态代码" IS '建档时新冠肺炎人员状态的分类在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."建档时处置措施代码" IS '建档时新冠肺炎处置措施的分类在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."最新人员状态代码" IS '最新新冠肺炎人员状态的分类在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."最新处置措施代码" IS '最新新冠肺炎处置措施的分类在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."建档医生工号" IS '首次为患者建立电子病历的人员的工号';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."建档医生姓名" IS '首次为患者建立电子病历的人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."建档日期" IS '建档完成时公元纪年日期的完整描述';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."症状代码" IS '新冠症状在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."症状名称" IS '新冠症状在特定编码体系中的名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."症状其他描述" IS '除上述症状外，其他症状的详细描述';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."湖北居住史标志" IS '标识患者是否有湖北旅居史的标志';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."接触史标志" IS '标识患者是否有新冠接触史的标志';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."发热日期" IS '患者出现发热的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."密接类型" IS '新冠肺炎密接类型的描述';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."返回日期" IS '患者返回当地的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."返回隔离日期" IS '患者返回当地后开始隔离的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."接触患者姓名" IS '接触患者人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."接触患者关系" IS '接触患者人员与患者的关系类别(如配偶、子女、父母等)在特定编码体系中的名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."暴露日期" IS '患者暴露的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."接触隔离日期" IS '患者接触暴露后隔离的日期';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."首诊日期" IS '患者第一次诊断的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."首诊机构代码" IS '按照某一特定编码规则赋予首诊机构的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."首诊机构名称" IS '首诊机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."确诊日期" IS '糖尿病确诊的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."确诊机构代码" IS '按照某一特定编码规则赋予确诊机构的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."确诊机构名称" IS '确诊机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新冠肺炎)专项档案"."确诊疾病" IS '确诊疾病诊断在特定编码体系中的名称。如存在多条诊断，是写主要诊断';
CREATE TABLE IF NOT EXISTS "(
新冠肺炎)专项档案" ("疑似收治日期" date DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "疑似收治机构代码" varchar (22) DEFAULT NULL,
 "疑似收治机构名称" varchar (70) DEFAULT NULL,
 "确诊收治日期" date DEFAULT NULL,
 "确诊收治机构代码" varchar (22) DEFAULT NULL,
 "确诊收治机构名称" varchar (70) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "专项档案标识号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "调查表流水号" varchar (64) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "家属姓名" varchar (50) DEFAULT NULL,
 "联系电话号码" varchar (20) DEFAULT NULL,
 "户籍地址" varchar (200) DEFAULT NULL,
 "户籍地址行政区划代码" varchar (18) DEFAULT NULL,
 "居住地址" varchar (200) DEFAULT NULL,
 "居住地-行政区划代码" varchar (18) DEFAULT NULL,
 "管理所在行政区划代码" varchar (18) DEFAULT NULL,
 "管理所在行政区划名称" varchar (200) DEFAULT NULL,
 "建档时人员状态代码" varchar (1) DEFAULT NULL,
 "建档时处置措施代码" varchar (1) DEFAULT NULL,
 "最新人员状态代码" varchar (1) DEFAULT NULL,
 "最新处置措施代码" varchar (1) DEFAULT NULL,
 "建档医生工号" varchar (20) DEFAULT NULL,
 "建档医生姓名" varchar (50) DEFAULT NULL,
 "建档日期" date DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "症状代码" varchar (50) DEFAULT NULL,
 "症状名称" varchar (100) DEFAULT NULL,
 "症状其他描述" varchar (500) DEFAULT NULL,
 "湖北居住史标志" varchar (1) DEFAULT NULL,
 "接触史标志" varchar (1) DEFAULT NULL,
 "发热日期" date DEFAULT NULL,
 "密接类型" varchar (50) DEFAULT NULL,
 "返回日期" date DEFAULT NULL,
 "返回隔离日期" date DEFAULT NULL,
 "接触患者姓名" varchar (50) DEFAULT NULL,
 "接触患者关系" varchar (100) DEFAULT NULL,
 "暴露日期" date DEFAULT NULL,
 "接触隔离日期" date DEFAULT NULL,
 "首诊日期" date DEFAULT NULL,
 "首诊机构代码" varchar (22) DEFAULT NULL,
 "首诊机构名称" varchar (70) DEFAULT NULL,
 "确诊日期" date DEFAULT NULL,
 "确诊机构代码" varchar (22) DEFAULT NULL,
 "确诊机构名称" varchar (70) DEFAULT NULL,
 "确诊疾病" varchar (100) DEFAULT NULL,
 CONSTRAINT "(新冠肺炎)专项档案"_"医疗机构代码"_"专项档案标识号"_PK PRIMARY KEY ("医疗机构代码",
 "专项档案标识号")
);


COMMENT ON TABLE "(新冠肺炎)调查结果" IS '新冠肺炎调查结果，含14天内旅居及接触情况调查结果';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."调查项目结果" IS '标识上述调查项目的是否结果';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."调查项目名称" IS '新冠肺炎调查项目在特定编码体系中的名称';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."调查项目代码" IS '新冠肺炎调查项目在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."调查表流水号" IS '按照某一特定编码规则赋予调查表的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."调查结果流水号" IS '按照某一特定编码规则赋予调查结果的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)调查结果"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
CREATE TABLE IF NOT EXISTS "(
新冠肺炎)调查结果" ("修改人员工号" varchar (20) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "调查项目结果" varchar (1) DEFAULT NULL,
 "调查项目名称" varchar (20) DEFAULT NULL,
 "调查项目代码" varchar (20) DEFAULT NULL,
 "调查表流水号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "调查结果流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 CONSTRAINT "(新冠肺炎)调查结果"_"调查结果流水号"_"医疗机构代码"_PK PRIMARY KEY ("调查结果流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "(新冠肺炎)转归记录" IS '新冠肺炎转归情况，包括转归情况、确诊机构、开始及结束发热时间、开始及结束隔离时间等';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."确诊疾病" IS '确诊疾病诊断在特定编码体系中的名称。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."确诊日期" IS '糖尿病确诊的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."确诊机构代码" IS '按照某一特定编码规则赋予确诊机构的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."确诊机构名称" IS '确诊机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."收治日期" IS '患者被医疗机构收治时的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."收治机构代码" IS '按照某一特定编码规则赋予收治机构的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."收治机构名称" IS '收治机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."开始发热日期" IS '开始发热时的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."结束发热日期" IS '结束发热时的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."开始隔离日期" IS '开始隔离时的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."结束隔离日期" IS '结束隔离时的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."出院日期" IS '完成出院手续办理时的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."死亡日期" IS '患者死亡当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."转归流水号" IS '按照某一特定编码规则赋予转归记录的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."专项档案标识号" IS '按照某一特定规则赋予专项档案的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."转归代码" IS '新冠病情转归的类型（如正常、确诊、治愈、死亡等）在特定编码体系中的代码';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."转归名称" IS '新冠病情转归的类型（如正常、确诊、治愈、死亡等）在特定编码体系中的名称';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."备注" IS '重要信息提示和补充说明';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."转归日期" IS '患者疾病转归时的公元纪年日期';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."转归医生工号" IS '转归医师的工号';
COMMENT ON COLUMN "(新冠肺炎)转归记录"."转归医生姓名" IS '转归医师在公安户籍管理部门正式登记注册的姓氏和名称';
CREATE TABLE IF NOT EXISTS "(
新冠肺炎)转归记录" ("确诊疾病" varchar (100) DEFAULT NULL,
 "确诊日期" date DEFAULT NULL,
 "确诊机构代码" varchar (22) DEFAULT NULL,
 "确诊机构名称" varchar (70) DEFAULT NULL,
 "收治日期" date DEFAULT NULL,
 "收治机构代码" varchar (22) DEFAULT NULL,
 "收治机构名称" varchar (70) DEFAULT NULL,
 "开始发热日期" date DEFAULT NULL,
 "结束发热日期" date DEFAULT NULL,
 "开始隔离日期" date DEFAULT NULL,
 "结束隔离日期" date DEFAULT NULL,
 "出院日期" date DEFAULT NULL,
 "死亡日期" date DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "转归流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "专项档案标识号" varchar (64) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "转归代码" varchar (1) DEFAULT NULL,
 "转归名称" varchar (1) DEFAULT NULL,
 "备注" varchar (1000) DEFAULT NULL,
 "转归日期" date DEFAULT NULL,
 "转归医生工号" varchar (20) DEFAULT NULL,
 "转归医生姓名" varchar (50) DEFAULT NULL,
 CONSTRAINT "(新冠肺炎)转归记录"_"医疗机构代码"_"转归流水号"_PK PRIMARY KEY ("医疗机构代码",
 "转归流水号")
);


COMMENT ON TABLE "(新生儿出生)医学证明" IS '新生儿及其父母亲的基本信息，以及新生儿出生孕周、身长、体重等医学信息';
COMMENT ON COLUMN "(新生儿出生)医学证明"."接生机构代码" IS '按照某一特定编码规则赋予接生机构的唯一标识';
COMMENT ON COLUMN "(新生儿出生)医学证明"."接生人员姓名" IS '接生人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."签发时间" IS '机构签发《出生医学证明》的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿出生)医学证明"."签发人员姓名" IS '签发人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."签发机构代码" IS '按照某一特定编码规则赋予签发机构的唯一标识';
COMMENT ON COLUMN "(新生儿出生)医学证明"."签发机构名称" IS '签发机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."领证人员姓名" IS '领证人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿出生)医学证明"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(新生儿出生)医学证明"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新生儿出生)医学证明"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿出生)医学证明"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿出生)医学证明"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(新生儿出生)医学证明"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新生儿出生)医学证明"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿出生)医学证明"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(新生儿出生)医学证明"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿出生)医学证明"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲证件类型代码" IS '母亲身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲民族名称" IS '母亲所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲民族代码" IS '母亲所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲国籍名称" IS '母亲所属国籍在特定编码体系中的名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲国籍代码" IS '母亲所属国籍在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲年龄" IS '母亲从出生当日公元纪年日起到计算当日止生存的时间长度';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲出生日期" IS '新生儿母亲出生当日的公元纪年日期';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲姓名" IS '新生儿母亲在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生身长(cm)" IS '新生儿出生后1h内身长的测量值，计量单位为cm';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生体重(g)" IS '新生儿出生后1h内体重的测量值，计量单位为g';
COMMENT ON COLUMN "(新生儿出生)医学证明"."健康状况代码" IS '个体健康状况在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生孕周(周)" IS '儿童出生时母亲妊娠时长的周数，计量单位为周';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生地-乡(镇、街道办事处)名称" IS '出生地址中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生地-乡(镇、街道办事处)代码" IS '出生地址中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生地-县(市、区)名称" IS '出生地址中的县或区名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生地-县(市、区)代码" IS '出生地址中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生地-市(地区、州)名称" IS '出生地址中的市、地区或州的名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生地-市(地区、州)代码" IS '出生地址中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生地-省(自治区、直辖市)名称" IS '出生地址中的省、自治区或直辖市名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生地-省(自治区、直辖市)代码" IS '出生地址中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生分钟" IS '新生儿出生累计时长，计量单位为分钟';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生小时" IS '新生儿出生累计时长，计量单位为小时';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "(新生儿出生)医学证明"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "(新生儿出生)医学证明"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "(新生儿出生)医学证明"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."姓名" IS '本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生证编号" IS '按照某一特定编码规则赋予本人出生医学证明的顺序号';
COMMENT ON COLUMN "(新生儿出生)医学证明"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(新生儿出生)医学证明"."证明流水号" IS '按照某一特定编码规则赋予本人出生医学证明记录的唯一标识';
COMMENT ON COLUMN "(新生儿出生)医学证明"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿出生)医学证明"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(新生儿出生)医学证明"."接生机构名称" IS '接生机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲证件类型名称" IS '母亲身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲证件号码" IS '母亲各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-详细地址" IS '母亲个人地址的详细描述';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-行政区划代码" IS '母亲住址的6位区县行政区划码+省统计局发布的3位乡镇编码和3位村编码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-省(自治区、直辖市)代码" IS '母亲住址中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-省(自治区、直辖市)名称" IS '母亲住址中的省、自治区或直辖市名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-市(地区、州)代码" IS '母亲住址中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-市(地区、州)名称" IS '母亲住址中的市、地区或州的名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-县(市、区)代码" IS '母亲住址中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-县(市、区)名称" IS '母亲住址中的县或区名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-乡(镇、街道办事处)代码" IS '母亲住址中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-乡(镇、街道办事处)名称" IS '母亲住址中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-村(街、路、弄等)代码" IS '母亲地址中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-村(街、路、弄等)名称" IS '母亲地址中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."母亲住址-门牌号码" IS '母亲地址中的门牌号码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲姓名" IS '新生儿父亲在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲出生日期" IS '新生儿父亲出生当日的公元纪年日期';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲年龄" IS '父亲从出生当日公元纪年日起到计算当日止生存的时间长度';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲国籍代码" IS '父亲所属国籍在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲国籍名称" IS '父亲所属国籍在特定编码体系中的名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲民族代码" IS '父亲所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲民族名称" IS '父亲所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲证件类型代码" IS '父亲身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲证件类型名称" IS '父亲身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲证件号码" IS '父亲各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-详细地址" IS '父亲个人地址的详细描述';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-行政区划代码" IS '父亲住址区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-省(自治区、直辖市)代码" IS '父亲住址中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-省(自治区、直辖市)名称" IS '父亲住址中的省、自治区或直辖市名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-市(地区、州)代码" IS '父亲住址中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-市(地区、州)名称" IS '父亲住址中的市、地区或州的名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-县(市、区)代码" IS '父亲住址中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-县(市、区)名称" IS '父亲住址中的县或区名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-乡(镇、街道办事处)代码" IS '父亲住址中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-乡(镇、街道办事处)名称" IS '父亲住址中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-村(街、路、弄等)代码" IS '父亲地址中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-村(街、路、弄等)名称" IS '父亲地址中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(新生儿出生)医学证明"."父亲住址-门牌号码" IS '父亲地址中的门牌号码';
COMMENT ON COLUMN "(新生儿出生)医学证明"."出生地点分类代码" IS '本人出生地点的类别在特定编码体系中的代码';
CREATE TABLE IF NOT EXISTS "(
新生儿出生)医学证明" ("接生机构代码" varchar (22) DEFAULT NULL,
 "接生人员姓名" varchar (50) DEFAULT NULL,
 "签发时间" timestamp DEFAULT NULL,
 "签发人员姓名" varchar (50) DEFAULT NULL,
 "签发机构代码" varchar (22) DEFAULT NULL,
 "签发机构名称" varchar (70) DEFAULT NULL,
 "领证人员姓名" varchar (50) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "母亲证件类型代码" varchar (2) DEFAULT NULL,
 "母亲民族名称" varchar (50) DEFAULT NULL,
 "母亲民族代码" varchar (2) DEFAULT NULL,
 "母亲国籍名称" varchar (50) DEFAULT NULL,
 "母亲国籍代码" varchar (3) DEFAULT NULL,
 "母亲年龄" decimal (2,
 0) DEFAULT NULL,
 "母亲出生日期" date DEFAULT NULL,
 "母亲姓名" varchar (50) DEFAULT NULL,
 "出生身长(cm)" decimal (4,
 1) DEFAULT NULL,
 "出生体重(g)" decimal (4,
 0) DEFAULT NULL,
 "健康状况代码" varchar (2) DEFAULT NULL,
 "出生孕周(周)" decimal (2,
 0) DEFAULT NULL,
 "出生地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "出生地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "出生地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "出生地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "出生地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "出生地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "出生地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "出生地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "出生分钟" decimal (2,
 0) DEFAULT NULL,
 "出生小时" decimal (2,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "出生证编号" varchar (10) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "证明流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "接生机构名称" varchar (70) DEFAULT NULL,
 "母亲证件类型名称" varchar (50) DEFAULT NULL,
 "母亲证件号码" varchar (18) DEFAULT NULL,
 "母亲住址-详细地址" varchar (200) DEFAULT NULL,
 "母亲住址-行政区划代码" varchar (9) DEFAULT NULL,
 "母亲住址-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "母亲住址-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "母亲住址-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "母亲住址-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "母亲住址-县(市、区)代码" varchar (6) DEFAULT NULL,
 "母亲住址-县(市、区)名称" varchar (70) DEFAULT NULL,
 "母亲住址-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "母亲住址-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "母亲住址-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "母亲住址-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "母亲住址-门牌号码" varchar (70) DEFAULT NULL,
 "父亲姓名" varchar (50) DEFAULT NULL,
 "父亲出生日期" date DEFAULT NULL,
 "父亲年龄" decimal (2,
 0) DEFAULT NULL,
 "父亲国籍代码" varchar (3) DEFAULT NULL,
 "父亲国籍名称" varchar (50) DEFAULT NULL,
 "父亲民族代码" varchar (2) DEFAULT NULL,
 "父亲民族名称" varchar (50) DEFAULT NULL,
 "父亲证件类型代码" varchar (2) DEFAULT NULL,
 "父亲证件类型名称" varchar (50) DEFAULT NULL,
 "父亲证件号码" varchar (18) DEFAULT NULL,
 "父亲住址-详细地址" varchar (200) DEFAULT NULL,
 "父亲住址-行政区划代码" varchar (9) DEFAULT NULL,
 "父亲住址-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "父亲住址-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "父亲住址-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "父亲住址-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "父亲住址-县(市、区)代码" varchar (6) DEFAULT NULL,
 "父亲住址-县(市、区)名称" varchar (70) DEFAULT NULL,
 "父亲住址-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "父亲住址-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "父亲住址-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "父亲住址-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "父亲住址-门牌号码" varchar (70) DEFAULT NULL,
 "出生地点分类代码" varchar (2) DEFAULT NULL,
 CONSTRAINT "(新生儿出生)医学证明"_"证明流水号"_"医疗机构代码"_PK PRIMARY KEY ("证明流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "(新生儿疾病筛查)确诊情况" IS '新生儿疾病筛查确诊情况，包括诊断机构、诊断方法、项目、结果等';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."诊断结果" IS '对患者罹患疾病诊断结果的结论性描述';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."诊断方法" IS '诊断方法的详细描述';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."诊断项目" IS '做出诊断的检查项目的详细描述';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."诊断人员姓名" IS '诊断人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."诊断人员工号" IS '诊断医师的工号';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."诊断机构名称" IS '诊断医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."诊断机构代码" IS '诊断机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."诊断时间" IS '诊断下达当日的公元纪年和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."标本编号" IS '按照某一特定编码规则赋予检查标本的顺序号';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)确诊情况"."修改人员工号" IS '修改人员的工号';
CREATE TABLE IF NOT EXISTS "(
新生儿疾病筛查)确诊情况" ("修改时间" timestamp DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "诊断结果" text,
 "诊断方法" varchar (100) DEFAULT NULL,
 "诊断项目" varchar (100) DEFAULT NULL,
 "诊断人员姓名" varchar (50) DEFAULT NULL,
 "诊断人员工号" varchar (20) DEFAULT NULL,
 "诊断机构名称" varchar (70) DEFAULT NULL,
 "诊断机构代码" varchar (22) DEFAULT NULL,
 "诊断时间" timestamp DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "标本编号" varchar (20) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 CONSTRAINT "(新生儿疾病筛查)确诊情况"_"标本编号"_"医疗机构代码"_PK PRIMARY KEY ("标本编号",
 "医疗机构代码")
);


COMMENT ON TABLE "(新生儿疾病筛查)采血情况" IS '新生儿疾病筛查采血情况，包括采血时间、方式、部位、验收情况等';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."标本编号" IS '按照某一特定编码规则赋予检查标本的顺序号';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."验收时间" IS '样本验收当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."验收情况名称" IS '样本验收情况在特定编码体系中的名称，如验收合格、验收不合格等';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."验收情况代码" IS '样本验收情况在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血部位其他" IS '其他采血部位描述';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血部位名称" IS '对个体采血部位在特定编码体系中对应的名称';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血部位代码" IS '对个体采血部位在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血方式其他" IS '其他采血方式的描述';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血方式名称" IS '对个体采血方式在特定编码体系中对应的名称';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血方式代码" IS '对个体采血方式在特定编码体系中的代码';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血分钟" IS '采血时间的分钟数';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血小时" IS '采血时间的小时数';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血日期" IS '采血当日的公元纪年日期';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血人员姓名" IS '采血人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血人员工号" IS '采血人员的工号';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血机构名称" IS '采血机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."采血机构代码" IS '按照某一特定编码规则赋予采血机构的唯一标识';
COMMENT ON COLUMN "(新生儿疾病筛查)采血情况"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
CREATE TABLE IF NOT EXISTS "(
新生儿疾病筛查)采血情况" ("修改标志" varchar (1) DEFAULT NULL,
 "标本编号" varchar (20) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "验收时间" timestamp DEFAULT NULL,
 "验收情况名称" varchar (50) DEFAULT NULL,
 "验收情况代码" varchar (2) DEFAULT NULL,
 "采血部位其他" varchar (100) DEFAULT NULL,
 "采血部位名称" varchar (50) DEFAULT NULL,
 "采血部位代码" varchar (2) DEFAULT NULL,
 "采血方式其他" varchar (100) DEFAULT NULL,
 "采血方式名称" varchar (50) DEFAULT NULL,
 "采血方式代码" varchar (2) DEFAULT NULL,
 "采血分钟" decimal (2,
 0) DEFAULT NULL,
 "采血小时" decimal (2,
 0) DEFAULT NULL,
 "采血日期" date DEFAULT NULL,
 "采血人员姓名" varchar (50) DEFAULT NULL,
 "采血人员工号" varchar (20) DEFAULT NULL,
 "采血机构名称" varchar (70) DEFAULT NULL,
 "采血机构代码" varchar (22) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 CONSTRAINT "(新生儿疾病筛查)采血情况"_"标本编号"_"医疗机构代码"_PK PRIMARY KEY ("标本编号",
 "医疗机构代码")
);


COMMENT ON TABLE "(签约)签约团队" IS '家庭医生签约团队信息，包括团队名称、负责人、服务区域、业务特长等';
COMMENT ON COLUMN "(签约)签约团队"."团队简介" IS '团队的简要介绍说明';
COMMENT ON COLUMN "(签约)签约团队"."服务区域" IS '家庭医生服务的区域范围描述';
COMMENT ON COLUMN "(签约)签约团队"."负责人姓名" IS '负责人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(签约)签约团队"."负责人工号" IS '负责人的工号';
COMMENT ON COLUMN "(签约)签约团队"."团队名称" IS '签约团队的名称描述';
COMMENT ON COLUMN "(签约)签约团队"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(签约)签约团队"."团队代码" IS '签约团队在家庭医生签约团队信息表中对应的代码';
COMMENT ON COLUMN "(签约)签约团队"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(签约)签约团队"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(签约)签约团队"."业务特长" IS '专业特长的详细描述';
COMMENT ON COLUMN "(签约)签约团队"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约团队"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约团队"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(签约)签约团队"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(签约)签约团队"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(签约)签约团队"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(签约)签约团队"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(签约)签约团队"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约团队"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(签约)签约团队"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(签约)签约团队"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(签约)签约团队"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(签约)签约团队"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
CREATE TABLE IF NOT EXISTS "(
签约)签约团队" ("团队简介" varchar (500) DEFAULT NULL,
 "服务区域" varchar (200) DEFAULT NULL,
 "负责人姓名" varchar (50) DEFAULT NULL,
 "负责人工号" varchar (20) DEFAULT NULL,
 "团队名称" varchar (100) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "团队代码" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "业务特长" varchar (500) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 CONSTRAINT "(签约)签约团队"_"团队代码"_"医疗机构代码"_PK PRIMARY KEY ("团队代码",
 "医疗机构代码")
);


COMMENT ON TABLE "(签约)签约服务项" IS '家庭医生签约服务项，包括服务项目、项目内容、次数、频率、收费标准等';
COMMENT ON COLUMN "(签约)签约服务项"."收费标准" IS '家庭医生签约服务包的收费标准金额，计量单位为人民币元';
COMMENT ON COLUMN "(签约)签约服务项"."频率" IS '家庭医生服务的频次，如一月一次';
COMMENT ON COLUMN "(签约)签约服务项"."次数" IS '家庭医生服务项目的总次数';
COMMENT ON COLUMN "(签约)签约服务项"."项目内容" IS '家庭医生签约服务项目的内容描述';
COMMENT ON COLUMN "(签约)签约服务项"."服务代码" IS '家庭医生服务在特定编码体系中的代码';
COMMENT ON COLUMN "(签约)签约服务项"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(签约)签约服务项"."服务项目名称" IS '服务项目在《签约服务包》表中的名称';
COMMENT ON COLUMN "(签约)签约服务项"."服务项目代码" IS '服务项目在《签约服务包》表中的代码';
COMMENT ON COLUMN "(签约)签约服务项"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(签约)签约服务项"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(签约)签约服务项"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约服务项"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(签约)签约服务项"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
CREATE TABLE IF NOT EXISTS "(
签约)签约服务项" ("收费标准" decimal (12,
 2) DEFAULT NULL,
 "频率" varchar (10) DEFAULT NULL,
 "次数" decimal (32,
 0) DEFAULT NULL,
 "项目内容" text,
 "服务代码" varchar (50) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "服务项目名称" varchar (50) DEFAULT NULL,
 "服务项目代码" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 CONSTRAINT "(签约)签约服务项"_"服务项目代码"_"医疗机构代码"_PK PRIMARY KEY ("服务项目代码",
 "医疗机构代码")
);


COMMENT ON TABLE "(精神病)患者用药指导" IS '精神病患者用药指导，包括药物名称、使用频率、剂量、途径等指导';
COMMENT ON COLUMN "(精神病)患者用药指导"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(精神病)患者用药指导"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(精神病)患者用药指导"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(精神病)患者用药指导"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(精神病)患者用药指导"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(精神病)患者用药指导"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(精神病)患者用药指导"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(精神病)患者用药指导"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(精神病)患者用药指导"."用药指导流水号" IS '按照一定编码规则赋予用药指导记录的顺序号';
COMMENT ON COLUMN "(精神病)患者用药指导"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(精神病)患者用药指导"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(精神病)患者用药指导"."补充表标识号" IS '按照某一特定规则赋予补充表的唯一标识';
COMMENT ON COLUMN "(精神病)患者用药指导"."随访流水号" IS '按照一定编码规则赋予随访记录的顺序号';
COMMENT ON COLUMN "(精神病)患者用药指导"."中药类别代码" IS '中药使用类别(如未使用、中成药、中草药、其他中药)在特定编码体系中的代码';
COMMENT ON COLUMN "(精神病)患者用药指导"."中药类别名称" IS '中药使用类别的标准名称，如未使用、中成药、中草药、其他中药等';
COMMENT ON COLUMN "(精神病)患者用药指导"."药物名称" IS '药品在特定编码体系中的名称。对于医疗机构制剂，此处填写医疗机构制剂名称';
COMMENT ON COLUMN "(精神病)患者用药指导"."药物使用频率名称" IS '单位时间内药物使用频次类别的标准名称，如每天两次、每周两次、睡前一次等';
COMMENT ON COLUMN "(精神病)患者用药指导"."药物使用剂量单位" IS '药物使用剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "(精神病)患者用药指导"."药物使用次剂量" IS '单次使用药物的剂量，按剂量单位计';
COMMENT ON COLUMN "(精神病)患者用药指导"."药物使用总剂量" IS '在一定时间段内使用药物的总量。根据单次剂量、频次计算得到的总量';
COMMENT ON COLUMN "(精神病)患者用药指导"."药物使用途径代码" IS '药物使用途径(如口服、静滴、喷喉等)在特定编码体系中的代码';
COMMENT ON COLUMN "(精神病)患者用药指导"."药物使用途径名称" IS '药物的给药途径在特定编码体系中的名称，如口服、静滴、喷喉等';
COMMENT ON COLUMN "(精神病)患者用药指导"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(精神病)患者用药指导"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(精神病)患者用药指导"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(精神病)患者用药指导"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(精神病)患者用药指导"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(精神病)患者用药指导"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(精神病)患者用药指导"."修改人员工号" IS '修改人员的工号';
CREATE TABLE IF NOT EXISTS "(
精神病)患者用药指导" ("修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "用药指导流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "补充表标识号" varchar (64) DEFAULT NULL,
 "随访流水号" varchar (64) DEFAULT NULL,
 "中药类别代码" varchar (2) DEFAULT NULL,
 "中药类别名称" varchar (50) DEFAULT NULL,
 "药物名称" varchar (50) DEFAULT NULL,
 "药物使用频率名称" varchar (20) DEFAULT NULL,
 "药物使用剂量单位" varchar (6) DEFAULT NULL,
 "药物使用次剂量" decimal (4,
 2) DEFAULT NULL,
 "药物使用总剂量" decimal (11,
 2) DEFAULT NULL,
 "药物使用途径代码" varchar (32) DEFAULT NULL,
 "药物使用途径名称" varchar (50) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 CONSTRAINT "(精神病)患者用药指导"_"医疗机构代码"_"用药指导流水号"_PK PRIMARY KEY ("医疗机构代码",
 "用药指导流水号")
);


COMMENT ON TABLE "(糖尿病)患者用药记录" IS '糖尿病患者用药记录，包括药物类别、名称、使用频率、剂量、途径等';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."管理卡标识号" IS '按照某一特定编码规则赋予管理卡的唯一标识号';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."随访流水号" IS '按照一定编码规则赋予随访记录的顺序号';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."中药类别代码" IS '中药使用类别(如未使用、中成药、中草药、其他中药)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."中药类别名称" IS '中药使用类别的标准名称，如未使用、中成药、中草药、其他中药等';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."药物名称" IS '药品在特定编码体系中的名称。对于医疗机构制剂，此处填写医疗机构制剂名称';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."药物使用频率名称" IS '单位时间内药物使用频次类别的标准名称，如每天两次、每周两次、睡前一次等';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."药物使用剂量单位" IS '药物使用剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."药物使用次剂量" IS '单次使用药物的剂量，按剂量单位计';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."药物使用总剂量" IS '在一定时间段内使用药物的总量。根据单次剂量、频次计算得到的总量';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."药物使用途径代码" IS '药物使用途径(如口服、静滴、喷喉等)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."药物使用途径名称" IS '药物的给药途径在特定编码体系中的名称，如口服、静滴、喷喉等';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."用药开始时间" IS '用药开始时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."用药停止时间" IS '用药结束时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."用药流水号" IS '按照一定编码规则赋予用药记录的顺序号';
COMMENT ON COLUMN "(糖尿病)患者用药记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
CREATE TABLE IF NOT EXISTS "(
糖尿病)患者用药记录" ("个人唯一标识号" varchar (64) DEFAULT NULL,
 "管理卡标识号" varchar (64) DEFAULT NULL,
 "随访流水号" varchar (64) DEFAULT NULL,
 "中药类别代码" varchar (2) DEFAULT NULL,
 "中药类别名称" varchar (50) DEFAULT NULL,
 "药物名称" varchar (50) DEFAULT NULL,
 "药物使用频率名称" varchar (20) DEFAULT NULL,
 "药物使用剂量单位" varchar (6) DEFAULT NULL,
 "药物使用次剂量" decimal (4,
 2) DEFAULT NULL,
 "药物使用总剂量" decimal (11,
 2) DEFAULT NULL,
 "药物使用途径代码" varchar (32) DEFAULT NULL,
 "药物使用途径名称" varchar (50) DEFAULT NULL,
 "用药开始时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "用药停止时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "用药流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 CONSTRAINT "(糖尿病)患者用药记录"_"医疗机构代码"_"用药流水号"_PK PRIMARY KEY ("医疗机构代码",
 "用药流水号")
);


COMMENT ON TABLE "(糖尿病)患者管理评估记录" IS '糖尿病患者评估信息，包括血压和血糖的检查值、分型、并发症等';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."摄盐改名名称" IS '摄盐改变程度名称，如未改变、部分改变、完全改变';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."摄盐改变代码" IS '摄盐改变程度(如未改变、部分改变、完全改变)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."饮食改变代码" IS '饮食改变程度(如未改变、部分改变、完全改变)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."饮酒改名名称" IS '饮酒改变程度名称，如未改变、部分改变、完全改变';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."饮酒改变代码" IS '饮酒改变程度(如未改变、部分改变、完全改变)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."吸烟改名名称" IS '吸烟改变程度名称，如未改变、部分改变、完全改变';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."吸烟改变代码" IS '吸烟改变程度(如未改变、部分改变、完全改变)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."辅助检查结果" IS '患者辅助检查结果的详细描述';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."辅助检查标志" IS '标识患者是否进行辅助检查的标志';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."并存临床情况" IS '并存的临床情况描述';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."确诊医院" IS '确诊医院的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."确诊日期" IS '糖尿病确诊的公元纪年日期';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."确诊方式" IS '对个体作出疾病诊断所采用的方法的详细描述';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."分型" IS '病例分型的名称及描述';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."糖化血红蛋白值(%)" IS '血液中糖化血红蛋白的测量值，计量单位为MMOL/L';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."随机血糖(mmol/L)" IS '任意时刻血液中葡萄糖定量检测结果值';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."餐后血糖(mmol/L)" IS '餐后血液中葡萄糖定量检测结果值，通常指餐后2小时的血糖值';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."空腹血糖(mmol/L)" IS '空腹状态下血液中葡萄糖定量检测结果值';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."评估医生姓名" IS '评估医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."评估医生工号" IS '评估医师的工号';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."评估机构名称" IS '评估机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."评估机构代码" IS '评估医疗机构的组织机构代码';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."评估日期" IS '对患者进行评估当日的公元纪年日期';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."管理卡标识号" IS '按照某一特定编码规则赋予管理卡的唯一标识号';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."评估流水号" IS '按照一定编码规则赋予产后访视记录的顺序号';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."运动改变代码" IS '运动改变程度(如未改变、部分改变、完全改变)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."运动改名名称" IS '运动改变程度名称，如未改变、部分改变、完全改变';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."心理状况改变代码" IS '心理状况改变程度(如未改变、部分改变、完全改变)在特定编码体系中的代码';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."心理状况改名名称" IS '心理状况改变程度名称，如未改变、部分改变、完全改变';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."指导建议" IS '描述医师针对病人情况而提出的指导建议';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."下次评估日期" IS '下次开展评估的公元纪年日期';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(糖尿病)患者管理评估记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
CREATE TABLE IF NOT EXISTS "(
糖尿病)患者管理评估记录" ("数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "摄盐改名名称" varchar (50) DEFAULT NULL,
 "摄盐改变代码" varchar (2) DEFAULT NULL,
 "饮食改变代码" varchar (2) DEFAULT NULL,
 "饮酒改名名称" varchar (50) DEFAULT NULL,
 "饮酒改变代码" varchar (2) DEFAULT NULL,
 "吸烟改名名称" varchar (50) DEFAULT NULL,
 "吸烟改变代码" varchar (2) DEFAULT NULL,
 "辅助检查结果" varchar (1000) DEFAULT NULL,
 "辅助检查标志" varchar (1) DEFAULT NULL,
 "并存临床情况" varchar (255) DEFAULT NULL,
 "确诊医院" varchar (70) DEFAULT NULL,
 "确诊日期" date DEFAULT NULL,
 "确诊方式" varchar (50) DEFAULT NULL,
 "分型" varchar (50) DEFAULT NULL,
 "糖化血红蛋白值(%)" decimal (3,
 1) DEFAULT NULL,
 "随机血糖(mmol/L)" decimal (3,
 1) DEFAULT NULL,
 "餐后血糖(mmol/L)" decimal (3,
 1) DEFAULT NULL,
 "空腹血糖(mmol/L)" decimal (3,
 1) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "评估医生姓名" varchar (50) DEFAULT NULL,
 "评估医生工号" varchar (20) DEFAULT NULL,
 "评估机构名称" varchar (70) DEFAULT NULL,
 "评估机构代码" varchar (22) DEFAULT NULL,
 "评估日期" date DEFAULT NULL,
 "管理卡标识号" varchar (64) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "评估流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "运动改变代码" varchar (2) DEFAULT NULL,
 "运动改名名称" varchar (50) DEFAULT NULL,
 "心理状况改变代码" varchar (2) DEFAULT NULL,
 "心理状况改名名称" varchar (50) DEFAULT NULL,
 "指导建议" varchar (500) DEFAULT NULL,
 "下次评估日期" date DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 CONSTRAINT "(糖尿病)患者管理评估记录"_"评估流水号"_"医疗机构代码"_PK PRIMARY KEY ("评估流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "(老年人)体质辨识" IS '老年人体质辨识，包括体质辨识标志、体质评分、主要体质、倾向体质、中医保健指导等';
COMMENT ON COLUMN "(老年人)体质辨识"."身体超重分类代码" IS '关于老年人是否身体超重的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."容易感到害怕分类代码" IS '关于老年人是否容易感到害怕的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."孤独失落分类代码" IS '关于老年人是否孤独失落的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."遇事紧张分类代码" IS '关于老年人是否遇事紧张的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."心情不愉快分类代码" IS '关于老年人是否心情不愉快的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."说话无力分类代码" IS '关于老年人是否说话物理的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."容易气短分类代码" IS '关于老年人是否容易气短的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."容易疲乏分类代码" IS '关于老年人是否容易疲乏的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."精力充沛分类代码" IS '关于老年人是否精力充沛的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."身份证号" IS '个体居民身份证上的唯一法定标识符';
COMMENT ON COLUMN "(老年人)体质辨识"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(老年人)体质辨识"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(老年人)体质辨识"."体质辨识流水号" IS '按照一定编码规则赋予评估记录的顺序号';
COMMENT ON COLUMN "(老年人)体质辨识"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(老年人)体质辨识"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(老年人)体质辨识"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(老年人)体质辨识"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(老年人)体质辨识"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(老年人)体质辨识"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(老年人)体质辨识"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(老年人)体质辨识"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(老年人)体质辨识"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(老年人)体质辨识"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(老年人)体质辨识"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(老年人)体质辨识"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(老年人)体质辨识"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(老年人)体质辨识"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(老年人)体质辨识"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(老年人)体质辨识"."评估日期" IS '评估日期的公元纪年日期';
COMMENT ON COLUMN "(老年人)体质辨识"."评估医生姓名" IS '评估医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(老年人)体质辨识"."评估医生工号" IS '评估医师的工号';
COMMENT ON COLUMN "(老年人)体质辨识"."其他中医保健指导" IS '中医药其他保健指导方案描述';
COMMENT ON COLUMN "(老年人)体质辨识"."中医保健指导名称" IS '中医药保健指导方案在特定编码体系中的名称，如情致调摄、饮食调养等';
COMMENT ON COLUMN "(老年人)体质辨识"."中医保健指导代码" IS '中医药保健指导方案在特定编码体系中的代码';
COMMENT ON COLUMN "(老年人)体质辨识"."倾向体质名称" IS '倾向中医体质在特定编码体系中的名称，包含平和质、气虚质、阳虚质、阴虚质、痰湿质、湿热质、血瘀质、气郁质和特禀质';
COMMENT ON COLUMN "(老年人)体质辨识"."倾向体质代码" IS '倾向中医体质的在特定编码体系中的代码';
COMMENT ON COLUMN "(老年人)体质辨识"."主要体质名称" IS '主要中医体质在特定编码体系中的名称，包含平和质、气虚质、阳虚质、阴虚质、痰湿质、湿热质、血瘀质、气郁质和特禀质';
COMMENT ON COLUMN "(老年人)体质辨识"."主要体质代码" IS '主要中医体质的在特定编码体系中的代码';
COMMENT ON COLUMN "(老年人)体质辨识"."平和质评分" IS '平和质评分值';
COMMENT ON COLUMN "(老年人)体质辨识"."特禀质评分" IS '特禀质评分值';
COMMENT ON COLUMN "(老年人)体质辨识"."气郁质评分" IS '气郁质评分值';
COMMENT ON COLUMN "(老年人)体质辨识"."血瘀质评分" IS '血瘀质评分值';
COMMENT ON COLUMN "(老年人)体质辨识"."湿热质评分" IS '湿热质评分值';
COMMENT ON COLUMN "(老年人)体质辨识"."痰湿质评分" IS '痰湿质评分值';
COMMENT ON COLUMN "(老年人)体质辨识"."阴虚质评分" IS '阴虚质评分值';
COMMENT ON COLUMN "(老年人)体质辨识"."阳虚质评分" IS '阳虚质评分值';
COMMENT ON COLUMN "(老年人)体质辨识"."气虚质评分" IS '气虚质评分值';
COMMENT ON COLUMN "(老年人)体质辨识"."舌下静脉淤紫分类代码" IS '关于老年人是否舌下静脉淤紫的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."舌苔厚腻分类代码" IS '关于老年人是否舌苔厚腻的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."大便干燥分类代码" IS '关于老年人是否大便干燥的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."解不尽分类代码" IS '关于老年人是否解不尽的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."不喜凉食分类代码" IS '关于老年人是否不喜凉食的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."腹部肥大分类代码" IS '关于老年人是否腹部肥大的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."口苦口臭分类代码" IS '关于老年人是否口苦口臭的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."口干咽燥分类代码" IS '关于老年人是否口干咽燥的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."皮肤湿疹分类代码" IS '关于老年人是否皮肤湿疹的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."面色晦暗分类代码" IS '关于老年人是否面色晦暗的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."面部鼻部油腻分类代码" IS '关于老年人是否面部鼻部油腻的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."肢体麻木分类代码" IS '关于老年人是否肢体麻木的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."皮肤口唇干分类代码" IS '关于老年人是否皮肤口唇干的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."皮肤一抓就红分类代码" IS '关于老年人是否皮肤一抓就红的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."青紫瘀斑分类代码" IS '关于老年人是否青紫瘀斑的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."皮肤容易荨麻疹分类代码" IS '关于老年人是否皮肤容易荨麻疹的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."容易过敏分类代码" IS '关于老年人是否容易过敏的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."口粘口腻分类代码" IS '关于老年人是否口粘口腻的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."鼻塞流涕分类代码" IS '关于老年人是否鼻塞流涕的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."容易患感冒分类代码" IS '关于老年人是否容易患感冒的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."不耐寒分类代码" IS '关于老年人是否不耐寒的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."胃脘怕冷分类代码" IS '关于老年人是否胃脘怕冷的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."手脚发凉分类代码" IS '关于老年人是否手脚发凉的中医体质辨识的分类代码';
COMMENT ON COLUMN "(老年人)体质辨识"."眼睛干涩分类代码" IS '关于老年人是否眼睛干涩的中医体质辨识的分类代码';
CREATE TABLE IF NOT EXISTS "(
老年人)体质辨识" ("身体超重分类代码" varchar (2) DEFAULT NULL,
 "容易感到害怕分类代码" varchar (2) DEFAULT NULL,
 "孤独失落分类代码" varchar (2) DEFAULT NULL,
 "遇事紧张分类代码" varchar (2) DEFAULT NULL,
 "心情不愉快分类代码" varchar (2) DEFAULT NULL,
 "说话无力分类代码" varchar (2) DEFAULT NULL,
 "容易气短分类代码" varchar (2) DEFAULT NULL,
 "容易疲乏分类代码" varchar (2) DEFAULT NULL,
 "精力充沛分类代码" varchar (2) DEFAULT NULL,
 "身份证号" varchar (18) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "体质辨识流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "评估日期" date DEFAULT NULL,
 "评估医生姓名" varchar (50) DEFAULT NULL,
 "评估医生工号" varchar (20) DEFAULT NULL,
 "其他中医保健指导" varchar (200) DEFAULT NULL,
 "中医保健指导名称" varchar (200) DEFAULT NULL,
 "中医保健指导代码" varchar (10) DEFAULT NULL,
 "倾向体质名称" varchar (200) DEFAULT NULL,
 "倾向体质代码" varchar (10) DEFAULT NULL,
 "主要体质名称" varchar (200) DEFAULT NULL,
 "主要体质代码" varchar (10) DEFAULT NULL,
 "平和质评分" decimal (6,
 0) DEFAULT NULL,
 "特禀质评分" decimal (6,
 0) DEFAULT NULL,
 "气郁质评分" decimal (6,
 0) DEFAULT NULL,
 "血瘀质评分" decimal (6,
 0) DEFAULT NULL,
 "湿热质评分" decimal (6,
 0) DEFAULT NULL,
 "痰湿质评分" decimal (6,
 0) DEFAULT NULL,
 "阴虚质评分" decimal (6,
 0) DEFAULT NULL,
 "阳虚质评分" decimal (6,
 0) DEFAULT NULL,
 "气虚质评分" decimal (6,
 0) DEFAULT NULL,
 "舌下静脉淤紫分类代码" varchar (2) DEFAULT NULL,
 "舌苔厚腻分类代码" varchar (2) DEFAULT NULL,
 "大便干燥分类代码" varchar (2) DEFAULT NULL,
 "解不尽分类代码" varchar (2) DEFAULT NULL,
 "不喜凉食分类代码" varchar (2) DEFAULT NULL,
 "腹部肥大分类代码" varchar (2) DEFAULT NULL,
 "口苦口臭分类代码" varchar (2) DEFAULT NULL,
 "口干咽燥分类代码" varchar (2) DEFAULT NULL,
 "皮肤湿疹分类代码" varchar (2) DEFAULT NULL,
 "面色晦暗分类代码" varchar (2) DEFAULT NULL,
 "面部鼻部油腻分类代码" varchar (2) DEFAULT NULL,
 "肢体麻木分类代码" varchar (2) DEFAULT NULL,
 "皮肤口唇干分类代码" varchar (2) DEFAULT NULL,
 "皮肤一抓就红分类代码" varchar (2) DEFAULT NULL,
 "青紫瘀斑分类代码" varchar (2) DEFAULT NULL,
 "皮肤容易荨麻疹分类代码" varchar (2) DEFAULT NULL,
 "容易过敏分类代码" varchar (2) DEFAULT NULL,
 "口粘口腻分类代码" varchar (2) DEFAULT NULL,
 "鼻塞流涕分类代码" varchar (2) DEFAULT NULL,
 "容易患感冒分类代码" varchar (2) DEFAULT NULL,
 "不耐寒分类代码" varchar (2) DEFAULT NULL,
 "胃脘怕冷分类代码" varchar (2) DEFAULT NULL,
 "手脚发凉分类代码" varchar (2) DEFAULT NULL,
 "眼睛干涩分类代码" varchar (2) DEFAULT NULL,
 CONSTRAINT "(老年人)体质辨识"_"体质辨识流水号"_"医疗机构代码"_PK PRIMARY KEY ("体质辨识流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "(老年人)自理能力评估" IS '老年人自理能力评估，包括进餐、梳洗、穿衣、如厕、活动等评分';
COMMENT ON COLUMN "(老年人)自理能力评估"."评估人" IS '评估医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(老年人)自理能力评估"."评估日期" IS '评估日期的公元纪年日期';
COMMENT ON COLUMN "(老年人)自理能力评估"."评估等级代码" IS '自己基本生活照料能力评估等级在特定编码体系中的代码';
COMMENT ON COLUMN "(老年人)自理能力评估"."总评分" IS '老年人自理能力的总评分值。其中，0～3 分者为可自理；4～8 分者为轻度依赖；\r\n9～18 分者为中度依赖；>=19 分者为不能自理';
COMMENT ON COLUMN "(老年人)自理能力评估"."活动评分" IS '老年人活动能力的评分值。其中0.独立完成所有活动；3.借助较小的外力或辅助装置能 完成站立、行走、上下楼梯等； 5. 借助较大的外力才能完成站立、行走，不能上下楼梯；10.卧床不起，活动完全需要帮助';
COMMENT ON COLUMN "(老年人)自理能力评估"."如厕评分" IS '老年人如厕能力的评分值。其中0.不需协助，可自控；3.偶尔失禁，但基本上能如厕或 使用便具； 5.经常失禁，在很多提示和协助 下尚能如厕或使用便具； 10.完全失禁，完全需要帮助';
COMMENT ON COLUMN "(老年人)自理能力评估"."穿衣评分" IS '老年人穿衣能力的评分值。其中0.独立完成；3.需要协助，在适当的时间内完 成部分穿衣；5.完全需要帮助';
COMMENT ON COLUMN "(老年人)自理能力评估"."梳洗评分" IS '老年人梳洗能力的评分值。其中，0.独立完成；1.能独立地洗头、梳头、洗脸、 刷牙、剃须等，洗澡需要协助；3.在协助下和适当的时间内，能完成部分梳洗活动；7.完全需要帮助';
COMMENT ON COLUMN "(老年人)自理能力评估"."进餐评分" IS '老年人进餐能力的评分值。其中0.独立完成；3.需要协助，如切碎、搅拌食物等；5.完全需要帮助';
COMMENT ON COLUMN "(老年人)自理能力评估"."身份证号" IS '个体居民身份证上的唯一法定标识符';
COMMENT ON COLUMN "(老年人)自理能力评估"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(老年人)自理能力评估"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(老年人)自理能力评估"."评估流水号" IS '按照一定编码规则赋予体质辨识评估记录的顺序号';
COMMENT ON COLUMN "(老年人)自理能力评估"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(老年人)自理能力评估"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(老年人)自理能力评估"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(老年人)自理能力评估"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(老年人)自理能力评估"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(老年人)自理能力评估"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(老年人)自理能力评估"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(老年人)自理能力评估"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(老年人)自理能力评估"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(老年人)自理能力评估"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(老年人)自理能力评估"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(老年人)自理能力评估"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(老年人)自理能力评估"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(老年人)自理能力评估"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(老年人)自理能力评估"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
CREATE TABLE IF NOT EXISTS "(
老年人)自理能力评估" ("评估人" varchar (50) DEFAULT NULL,
 "评估日期" date DEFAULT NULL,
 "评估等级代码" varchar (2) DEFAULT NULL,
 "总评分" decimal (2,
 0) DEFAULT NULL,
 "活动评分" decimal (6,
 0) DEFAULT NULL,
 "如厕评分" decimal (6,
 0) DEFAULT NULL,
 "穿衣评分" decimal (2,
 0) DEFAULT NULL,
 "梳洗评分" decimal (6,
 0) DEFAULT NULL,
 "进餐评分" decimal (2,
 0) DEFAULT NULL,
 "身份证号" varchar (18) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "评估流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 CONSTRAINT "(老年人)自理能力评估"_"评估流水号"_"医疗机构代码"_PK PRIMARY KEY ("评估流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "(脑卒中)专项档案" IS '脑卒中患者基本信息，包括人口统计学信息、疾病危险因素、疾病类型、病史、并发症等';
COMMENT ON COLUMN "(脑卒中)专项档案"."职业类别名称" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."学历代码" IS '个体受教育最高程度的类别(如研究生教育、大学本科、专科教育等)在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."学历名称" IS '个体受教育最高程度的类别标准名称，如研究生教育、大学本科、专科教育等';
COMMENT ON COLUMN "(脑卒中)专项档案"."婚姻状况代码" IS '当前婚姻状况(已婚、未婚、初婚等)在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."婚姻状况名称" IS '当前婚姻状况的标准名称，如已婚、未婚、初婚等';
COMMENT ON COLUMN "(脑卒中)专项档案"."医疗费用支付方式代码" IS '患者医疗费用支付方式类别(如城镇职工基本医疗保险、城镇居民基本医疗保险等)在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."医疗费用支付方式名称" IS '患者医疗费用支付方式类别在特定编码体系中的名称，如城镇职工基本医疗保险、城镇居民基本医疗保险等';
COMMENT ON COLUMN "(脑卒中)专项档案"."医疗费用支付方式其他" IS '除上述医疗费用支付方式外，其他支付方式的名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."手机号码" IS '居民本人的手机号码';
COMMENT ON COLUMN "(脑卒中)专项档案"."联系电话号码" IS '居民本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(脑卒中)专项档案"."工作单位名称" IS '个体工作单位的组织机构名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-详细地址" IS '户籍地址的详细描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-行政区划代码" IS '户籍地址所在区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-省(自治区、直辖市)代码" IS '户籍登记所在地址的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-省(自治区、直辖市)名称" IS '户籍登记所在地址的省、自治区或直辖市名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-市(地区、州)代码" IS '户籍登记所在地址的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-市(地区、州)名称" IS '户籍登记所在地址的市、地区或州的名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-县(市、区)代码" IS '户籍登记所在地址的县(区)的在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-县(市、区)名称" IS '户籍登记所在地址的县(市、区)的名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-乡(镇、街道办事处)代码" IS '户籍登记所在地址的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(脑卒中)专项档案"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(脑卒中)专项档案"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(脑卒中)专项档案"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(脑卒中)专项档案"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(脑卒中)专项档案"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(脑卒中)专项档案"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(脑卒中)专项档案"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."终止理由其他" IS '患者终止管理的其他原因描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."终止理由名称" IS '患者终止管理的原因类别在特定编码体系中的名称，如死亡、迁出、失访等';
COMMENT ON COLUMN "(脑卒中)专项档案"."终止理由代码" IS '患者终止管理的原因类别在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."终止日期" IS '高血压患者终止管理当日的公元纪年日期';
COMMENT ON COLUMN "(脑卒中)专项档案"."终止管理标志" IS '标识该高血压患者是否终止管理';
COMMENT ON COLUMN "(脑卒中)专项档案"."首次发病标志" IS '标识患者是否首次发烧的标志';
COMMENT ON COLUMN "(脑卒中)专项档案"."保卡单位" IS '保卡单位的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(脑卒中)专项档案"."保卡日期" IS '健康保健完成时的公元纪年日期';
COMMENT ON COLUMN "(脑卒中)专项档案"."保卡医生姓名" IS '保卡医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."确诊医院名称" IS '确诊医院的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."确诊医院类型" IS '确诊医院的机构类型';
COMMENT ON COLUMN "(脑卒中)专项档案"."确诊日期" IS '糖尿病确诊的公元纪年日期';
COMMENT ON COLUMN "(脑卒中)专项档案"."发病日期" IS '疾病发病时的公元纪年日期';
COMMENT ON COLUMN "(脑卒中)专项档案"."病史摘要" IS '对患者病情摘要的详细描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."病史" IS '对个体既往健康状况和疾病(含外伤)的详细描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."后遗症详述" IS '相关后遗症的详细描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."后遗症" IS '相关后遗症的描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."并发症详述" IS '相关并发症的详细描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."并发症" IS '相关并发症描述，多项时使用“；”分割';
COMMENT ON COLUMN "(脑卒中)专项档案"."首要症状" IS '对个体首先出现症状的详细描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."卒中发病时间与CT/磁共振检查时间间隔" IS '卒中发病时间与CT/磁共振检查时间间隔的具体描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."疾病类型" IS '疾病类型的描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."ICD诊断代码" IS '平台中心ICD诊断代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."实足年龄" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "(脑卒中)专项档案"."住院号" IS '按照某一特定编码规则赋予住院对象的顺序号';
COMMENT ON COLUMN "(脑卒中)专项档案"."门(急)诊号" IS '按照某一特定编码规则赋予门(急)诊就诊对象的顺序号';
COMMENT ON COLUMN "(脑卒中)专项档案"."身体活动受限情况" IS '身体活动受限情况的描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."危险因素描述" IS '危险因素的详细描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."建卡医生姓名" IS '建卡医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."建卡医生工号" IS '建卡医师的工号';
COMMENT ON COLUMN "(脑卒中)专项档案"."建卡机构名称" IS '建卡机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."建卡机构代码" IS '按照某一特定编码规则赋予建卡机构的唯一标识';
COMMENT ON COLUMN "(脑卒中)专项档案"."建卡日期" IS '完成建卡时的公元纪年日期';
COMMENT ON COLUMN "(脑卒中)专项档案"."报告卡标识号" IS '按照某一特定规则赋予报告卡的唯一标识';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-邮政编码" IS '现住地址中所在行政区的邮政编码';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-门牌号码" IS '本人现住地址中的门牌号码';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-村(街、路、弄等)名称" IS '本人现住地址中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-村(街、路、弄等)代码" IS '本人现住地址中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-居委名称" IS '现住地址所属的居民委员会名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-居委代码" IS '现住地址所属的居民委员会在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-乡(镇、街道办事处)名称" IS '本人现住地址中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-乡(镇、街道办事处)代码" IS '本人现住地址中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-县(市、区)名称" IS '现住地址中的县或区名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-县(市、区)代码" IS '现住地址中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-市(地区、州)名称" IS '现住地址中的市、地区或州的名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-市(地区、州)代码" IS '现住地址中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-省(自治区、直辖市)名称" IS '现住地址中的省、自治区或直辖市名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-省(自治区、直辖市)代码" IS '现住地址中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-行政区划代码" IS '居住地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(脑卒中)专项档案"."居住地-详细地址" IS '现住地址的详细描述';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-邮政编码" IS '户籍地址所在行政区的邮政编码';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-门牌号码" IS '户籍登记所在地址的门牌号码';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-村(街、路、弄等)名称" IS '户籍登记所在地址的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-村(街、路、弄等)代码" IS '户籍登记所在地址的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-居委名称" IS '户籍登记所在地址的居民委员会名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-居委代码" IS '户籍登记所在地址的居民委员会在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."户籍地-乡(镇、街道办事处)名称" IS '户籍登记所在地址的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(脑卒中)专项档案"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(脑卒中)专项档案"."专项档案标识号" IS '按照某一特定规则赋予专项档案的唯一标识';
COMMENT ON COLUMN "(脑卒中)专项档案"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(脑卒中)专项档案"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(脑卒中)专项档案"."姓名" IS '本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "(脑卒中)专项档案"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "(脑卒中)专项档案"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "(脑卒中)专项档案"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)专项档案"."民族名称" IS '所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "(脑卒中)专项档案"."职业类别代码" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的代码';
CREATE TABLE IF NOT EXISTS "(
脑卒中)专项档案" ("职业类别名称" varchar (60) DEFAULT NULL,
 "学历代码" varchar (5) DEFAULT NULL,
 "学历名称" varchar (50) DEFAULT NULL,
 "婚姻状况代码" varchar (2) DEFAULT NULL,
 "婚姻状况名称" varchar (50) DEFAULT NULL,
 "医疗费用支付方式代码" varchar (30) DEFAULT NULL,
 "医疗费用支付方式名称" varchar (100) DEFAULT NULL,
 "医疗费用支付方式其他" varchar (100) DEFAULT NULL,
 "手机号码" varchar (20) DEFAULT NULL,
 "联系电话号码" varchar (20) DEFAULT NULL,
 "工作单位名称" varchar (70) DEFAULT NULL,
 "户籍地-详细地址" varchar (200) DEFAULT NULL,
 "户籍地-行政区划代码" varchar (12) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "户籍地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "户籍地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "户籍地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "户籍地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "终止理由其他" varchar (100) DEFAULT NULL,
 "终止理由名称" varchar (50) DEFAULT NULL,
 "终止理由代码" varchar (2) DEFAULT NULL,
 "终止日期" date DEFAULT NULL,
 "终止管理标志" varchar (1) DEFAULT NULL,
 "首次发病标志" varchar (1) DEFAULT NULL,
 "保卡单位" varchar (200) DEFAULT NULL,
 "保卡日期" date DEFAULT NULL,
 "保卡医生姓名" varchar (50) DEFAULT NULL,
 "确诊医院名称" varchar (200) DEFAULT NULL,
 "确诊医院类型" varchar (10) DEFAULT NULL,
 "确诊日期" date DEFAULT NULL,
 "发病日期" date DEFAULT NULL,
 "病史摘要" varchar (500) DEFAULT NULL,
 "病史" varchar (200) DEFAULT NULL,
 "后遗症详述" varchar (500) DEFAULT NULL,
 "后遗症" varchar (200) DEFAULT NULL,
 "并发症详述" varchar (500) DEFAULT NULL,
 "并发症" varchar (200) DEFAULT NULL,
 "首要症状" varchar (200) DEFAULT NULL,
 "卒中发病时间与CT/磁共振检查时间间隔" varchar (10) DEFAULT NULL,
 "疾病类型" varchar (10) DEFAULT NULL,
 "ICD诊断代码" varchar (20) DEFAULT NULL,
 "实足年龄" decimal (6,
 0) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "门(急)诊号" varchar (32) DEFAULT NULL,
 "身体活动受限情况" varchar (255) DEFAULT NULL,
 "危险因素描述" varchar (255) DEFAULT NULL,
 "建卡医生姓名" varchar (50) DEFAULT NULL,
 "建卡医生工号" varchar (20) DEFAULT NULL,
 "建卡机构名称" varchar (70) DEFAULT NULL,
 "建卡机构代码" varchar (22) DEFAULT NULL,
 "建卡日期" date DEFAULT NULL,
 "报告卡标识号" varchar (50) DEFAULT NULL,
 "居住地-邮政编码" varchar (6) DEFAULT NULL,
 "居住地-门牌号码" varchar (70) DEFAULT NULL,
 "居住地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "居住地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "居住地-居委名称" varchar (32) DEFAULT NULL,
 "居住地-居委代码" varchar (12) DEFAULT NULL,
 "居住地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "居住地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "居住地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "居住地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "居住地-市(地区、州)名称" varchar (32) DEFAULT NULL,
 "居住地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "居住地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "居住地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "居住地-行政区划代码" varchar (12) DEFAULT NULL,
 "居住地-详细地址" varchar (200) DEFAULT NULL,
 "户籍地-邮政编码" varchar (6) DEFAULT NULL,
 "户籍地-门牌号码" varchar (100) DEFAULT NULL,
 "户籍地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "户籍地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "户籍地-居委名称" varchar (32) DEFAULT NULL,
 "户籍地-居委代码" varchar (12) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "专项档案标识号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "民族代码" varchar (2) DEFAULT NULL,
 "民族名称" varchar (50) DEFAULT NULL,
 "职业类别代码" varchar (4) DEFAULT NULL,
 CONSTRAINT "(脑卒中)专项档案"_"医疗机构代码"_"专项档案标识号"_PK PRIMARY KEY ("医疗机构代码",
 "专项档案标识号")
);


COMMENT ON TABLE "(脑卒中)阶段性评估报告" IS '脑卒中患者阶段性评估，包括药物和非药物治疗效果、病情转归、评估结果、指导建议等';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."评估结果" IS '评估结果的详细描述';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."指导建议" IS '描述医师针对病人情况而提出的指导建议';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."评估日期" IS '对患者进行评估当日的公元纪年日期';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."评估医生工号" IS '评估医师的工号';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."评估医生姓名" IS '评估医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."评估机构编号" IS '评估医疗机构的组织机构代码';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."评估机构名称" IS '评估机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."评估流水号" IS '按照一定编码规则赋予产后访视记录的顺序号';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."专项档案标识号" IS '按照某一特定规则赋予专项档案的唯一标识';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."药物治疗效果" IS '药物治疗效果的详细描述';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."非药物治疗效果" IS '非药物治疗效果的详细描述';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."药物依从性" IS '患者服药依从性情况的描述';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."危险因素控制效果" IS '危险因素控制效果的详细描述';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."康复计划执行" IS '康复计划执行情况的描述';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."脑卒中功能评价" IS '针对患者脑卒中功能的详细评价';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."Rankin评分" IS '对Rankin评分的简要说明';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."Rankin评分值" IS '医生针对患者的Rankin评分值';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."康复效果评价" IS '针对患者康复效果的详细评价';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."病情转归代码" IS '疾病治疗结果的类别(如治愈、好转、稳定、恶化等)在特定编码体系中的代码';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."调整康复计划标志" IS '标识对患者进行康复计划调整的标志';
COMMENT ON COLUMN "(脑卒中)阶段性评估报告"."调整康复计划" IS '调整的康复计划具体内容';
CREATE TABLE IF NOT EXISTS "(
脑卒中)阶段性评估报告" ("评估结果" varchar (500) DEFAULT NULL,
 "指导建议" varchar (500) DEFAULT NULL,
 "评估日期" date DEFAULT NULL,
 "评估医生工号" varchar (20) DEFAULT NULL,
 "评估医生姓名" varchar (50) DEFAULT NULL,
 "评估机构编号" varchar (22) DEFAULT NULL,
 "评估机构名称" varchar (70) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "评估流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "专项档案标识号" varchar (64) DEFAULT NULL,
 "药物治疗效果" varchar (10) DEFAULT NULL,
 "非药物治疗效果" varchar (10) DEFAULT NULL,
 "药物依从性" varchar (10) DEFAULT NULL,
 "危险因素控制效果" varchar (10) DEFAULT NULL,
 "康复计划执行" varchar (10) DEFAULT NULL,
 "脑卒中功能评价" varchar (500) DEFAULT NULL,
 "Rankin评分" varchar (10) DEFAULT NULL,
 "Rankin评分值" decimal (6,
 0) DEFAULT NULL,
 "康复效果评价" varchar (10) DEFAULT NULL,
 "病情转归代码" varchar (1) DEFAULT NULL,
 "调整康复计划标志" varchar (1) DEFAULT NULL,
 "调整康复计划" varchar (500) DEFAULT NULL,
 CONSTRAINT "(脑卒中)阶段性评估报告"_"医疗机构代码"_"评估流水号"_PK PRIMARY KEY ("医疗机构代码",
 "评估流水号")
);


COMMENT ON TABLE "(高血压)患者管理卡" IS '高血压患者基本信息，包括人口统计学信息、建卡信息、高血压家族史、高血压类型、生活习惯等';
COMMENT ON COLUMN "(高血压)患者管理卡"."并发症" IS '高血压的相关并发症描述，多项时使用“；”分割';
COMMENT ON COLUMN "(高血压)患者管理卡"."生活自理能力代码" IS '自己基本生活照料能力在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."生活自理能力名称" IS '自己基本生活照料能力在特定编码体系中的名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."终止管理标志" IS '标识该高血压患者是否终止管理';
COMMENT ON COLUMN "(高血压)患者管理卡"."终止日期" IS '高血压患者终止管理当日的公元纪年日期';
COMMENT ON COLUMN "(高血压)患者管理卡"."终止理由代码" IS '患者终止管理的原因类别在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."终止理由名称" IS '患者终止管理的原因类别在特定编码体系中的名称，如死亡、迁出、失访等';
COMMENT ON COLUMN "(高血压)患者管理卡"."终止理由其他" IS '患者终止管理的其他原因描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."确诊医院" IS '疾病确诊的医院组织机构名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."确诊时间" IS '确定诊断下达当日的公元纪年和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."家族史" IS '患者3代以内有血缘关系的家族成员中所患遗传疾病史的描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."个人史" IS '患者个人生活习惯及有无烟、酒、药物等嗜好,职业与工作条件及有无工业毒物、粉尘、放射性物质接触史,有无冶游史的描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."过敏史" IS '患者既往发生过敏情况的详细描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."脑血管疾病描述" IS '患者脑血管疾病信息的详细描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."心脏疾病描述" IS '患者心脏疾病信息的详细描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."肾脏疾病描述" IS '患者脑肾脏疾病信息的详细描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."血管疾病描述" IS '患者血管疾病信息的详细描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."高血压性视网膜突变描述" IS '患者高血压性视网膜突变信息的详细描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "(高血压)患者管理卡"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(高血压)患者管理卡"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(高血压)患者管理卡"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "(高血压)患者管理卡"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "(高血压)患者管理卡"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(高血压)患者管理卡"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "(高血压)患者管理卡"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."饮酒情况代码" IS '患者饮酒频率分类(如从不、偶尔、少于1d/月等)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."吸咽情况名称" IS '个体现在吸烟频率的标准名称，如现在每天吸；现在吸，但不是每天吸；过去吸，现在不吸；从不吸';
COMMENT ON COLUMN "(高血压)患者管理卡"."吸咽情况代码" IS '个体现在吸烟频率(如现在每天吸；现在吸，但不是每天吸；过去吸，现在不吸；从不吸)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."病例来源名称" IS '病例来源在特定编码体系中的名称，如门诊就诊、健康档案、首诊测压等';
COMMENT ON COLUMN "(高血压)患者管理卡"."病例来源代码" IS '病例来源在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."高血压类型名称" IS '高血压的标准类型，即原发性高血压或继发性高血压';
COMMENT ON COLUMN "(高血压)患者管理卡"."高血压类型代码" IS '高血压类型(原发性高血压、继发性高血压)在标准码编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."业务管理机构名称" IS '业务管理机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(高血压)患者管理卡"."业务管理机构代码" IS '业务管理机构的组织机构代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."建卡医生姓名" IS '建卡医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."建卡医生工号" IS '建卡医师的工号';
COMMENT ON COLUMN "(高血压)患者管理卡"."建卡机构名称" IS '建卡机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."建卡机构代码" IS '按照某一特定编码规则赋予建卡机构的唯一标识';
COMMENT ON COLUMN "(高血压)患者管理卡"."建卡日期" IS '完成建卡时的公元纪年日期';
COMMENT ON COLUMN "(高血压)患者管理卡"."表单编号" IS '按照某一特定规则赋予表单的唯一标识';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-邮政编码" IS '居住地址的邮政编码';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-门牌号码" IS '儿童居住地中的门牌号码';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-村(街、路、弄等)名称" IS '儿童居住地中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-村(街、路、弄等)代码" IS '儿童居住地中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-居委名称" IS '居住地址的居民委员会名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-居委代码" IS '居住地址的居民委员会在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-乡(镇、街道办事处)名称" IS '儿童居住地中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-乡(镇、街道办事处)代码" IS '儿童居住地中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-县(市、区)名称" IS '儿童居住地中的县或区名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-县(市、区)代码" IS '儿童居住地中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-市(地区、州)名称" IS '儿童居住地中的市、地区或州的名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-市(地区、州)代码" IS '儿童居住地中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-省(自治区、直辖市)名称" IS '儿童居住地中的省、自治区或直辖市名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-省(自治区、直辖市)代码" IS '儿童居住地中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-行政区划代码" IS '儿童居住地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(高血压)患者管理卡"."居住地-详细地址" IS '儿童居住地址的详细描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-邮政编码" IS '户籍地址地址的邮政编码';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-门牌号码" IS '儿童户籍地中的门牌号码';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-村(街、路、弄等)名称" IS '儿童户籍地中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-村(街、路、弄等)代码" IS '儿童户籍地中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-居委名称" IS '户籍登记所在地址的居民委员会名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-居委代码" IS '户籍登记所在地址的居民委员会在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-乡(镇、街道办事处)名称" IS '儿童户籍地中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-乡(镇、街道办事处)代码" IS '儿童户籍地中的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-县(市、区)名称" IS '儿童户籍地中的县或区名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-县(市、区)代码" IS '儿童户籍地中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-市(地区、州)名称" IS '儿童户籍地中的市、地区或州的名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-市(地区、州)代码" IS '儿童户籍地中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-省(自治区、直辖市)名称" IS '儿童户籍地中的省、自治区或直辖市名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-省(自治区、直辖市)代码" IS '儿童户籍地中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-行政区划代码" IS '儿童户籍地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "(高血压)患者管理卡"."户籍地-详细地址" IS '儿童户籍地址的详细描述';
COMMENT ON COLUMN "(高血压)患者管理卡"."工作单位名称" IS '个体工作单位或学校的组织机构名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."联系电话号码" IS '本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "(高血压)患者管理卡"."手机号码" IS '居民本人的手机号码';
COMMENT ON COLUMN "(高血压)患者管理卡"."医疗费用支付方式其他" IS '除上述医疗费用支付方式外，其他支付方式的名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."医疗费用支付方式名称" IS '患者医疗费用支付方式类别在特定编码体系中的名称，如城镇职工基本医疗保险、城镇居民基本医疗保险等';
COMMENT ON COLUMN "(高血压)患者管理卡"."医疗费用支付方式代码" IS '患者医疗费用支付方式类别(如城镇职工基本医疗保险、城镇居民基本医疗保险等)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."婚姻状况名称" IS '当前婚姻状况的标准名称，如已婚、未婚、初婚等';
COMMENT ON COLUMN "(高血压)患者管理卡"."婚姻状况代码" IS '当前婚姻状况(已婚、未婚、初婚等)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."学历名称" IS '个体受教育最高程度的类别标准名称，如研究生教育、大学本科、专科教育等';
COMMENT ON COLUMN "(高血压)患者管理卡"."学历代码" IS '个体受教育最高程度的类别(如研究生教育、大学本科、专科教育等)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."职业类别名称" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."职业类别代码" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."民族名称" IS '所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "(高血压)患者管理卡"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "(高血压)患者管理卡"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "(高血压)患者管理卡"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "(高血压)患者管理卡"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "(高血压)患者管理卡"."管理卡标识号" IS '按照某一特定编码规则赋予管理卡的唯一标识号';
COMMENT ON COLUMN "(高血压)患者管理卡"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "(高血压)患者管理卡"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "(高血压)患者管理卡"."饮酒情况名称" IS '患者饮酒频率分类的标准名称，如从不、偶尔、少于1d/月等';
COMMENT ON COLUMN "(高血压)患者管理卡"."体育锻炼代码" IS '个体最近1个月主动运动的频率在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."体育锻炼名称" IS '个体最近1个月主动运动的频率的标准名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."饮食习惯代码" IS '个体饮食习惯在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."饮食习惯名称" IS '个体饮食习惯的标准名称，如荤素均衡、荤食为主、素食为主、嗜盐、嗜油等';
COMMENT ON COLUMN "(高血压)患者管理卡"."心理状况代码" IS '个体心理状况的分类在特定编码体系中的代码';
COMMENT ON COLUMN "(高血压)患者管理卡"."心理状况名称" IS '个体心理状况的分类在特定编码体系中的名称';
COMMENT ON COLUMN "(高血压)患者管理卡"."身高(cm)" IS '个体身高的测量值，计量单位为cm';
COMMENT ON COLUMN "(高血压)患者管理卡"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "(高血压)患者管理卡"."体质指数" IS '根据体重(kg)除以身高平方(m2)计算出的指数';
COMMENT ON COLUMN "(高血压)患者管理卡"."腰围(cm)" IS '腰围测量值,计量单位为cm';
COMMENT ON COLUMN "(高血压)患者管理卡"."臀围(cm)" IS '受检者臀部周长的测量值，计量单位为cm';
COMMENT ON COLUMN "(高血压)患者管理卡"."腰臀围比" IS '腰围与臀围的比值';
COMMENT ON COLUMN "(高血压)患者管理卡"."心率(次/min)" IS '心脏搏动频率的测量值,计量单位为次/min';
COMMENT ON COLUMN "(高血压)患者管理卡"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "(高血压)患者管理卡"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
CREATE TABLE IF NOT EXISTS "(
高血压)患者管理卡" ("并发症" varchar (200) DEFAULT NULL,
 "生活自理能力代码" varchar (2) DEFAULT NULL,
 "生活自理能力名称" varchar (50) DEFAULT NULL,
 "终止管理标志" varchar (1) DEFAULT NULL,
 "终止日期" date DEFAULT NULL,
 "终止理由代码" varchar (2) DEFAULT NULL,
 "终止理由名称" varchar (50) DEFAULT NULL,
 "终止理由其他" varchar (100) DEFAULT NULL,
 "确诊医院" varchar (50) DEFAULT NULL,
 "确诊时间" timestamp DEFAULT NULL,
 "家族史" varchar (100) DEFAULT NULL,
 "个人史" text,
 "过敏史" text,
 "脑血管疾病描述" varchar (255) DEFAULT NULL,
 "心脏疾病描述" varchar (100) DEFAULT NULL,
 "肾脏疾病描述" varchar (100) DEFAULT NULL,
 "血管疾病描述" varchar (255) DEFAULT NULL,
 "高血压性视网膜突变描述" varchar (255) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "饮酒情况代码" varchar (2) DEFAULT NULL,
 "吸咽情况名称" varchar (50) DEFAULT NULL,
 "吸咽情况代码" varchar (2) DEFAULT NULL,
 "病例来源名称" varchar (50) DEFAULT NULL,
 "病例来源代码" varchar (2) DEFAULT NULL,
 "高血压类型名称" varchar (50) DEFAULT NULL,
 "高血压类型代码" varchar (2) DEFAULT NULL,
 "业务管理机构名称" varchar (70) DEFAULT NULL,
 "业务管理机构代码" varchar (22) DEFAULT NULL,
 "建卡医生姓名" varchar (50) DEFAULT NULL,
 "建卡医生工号" varchar (20) DEFAULT NULL,
 "建卡机构名称" varchar (70) DEFAULT NULL,
 "建卡机构代码" varchar (22) DEFAULT NULL,
 "建卡日期" date DEFAULT NULL,
 "表单编号" varchar (20) DEFAULT NULL,
 "居住地-邮政编码" varchar (6) DEFAULT NULL,
 "居住地-门牌号码" varchar (70) DEFAULT NULL,
 "居住地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "居住地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "居住地-居委名称" varchar (32) DEFAULT NULL,
 "居住地-居委代码" varchar (12) DEFAULT NULL,
 "居住地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "居住地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "居住地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "居住地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "居住地-市(地区、州)名称" varchar (32) DEFAULT NULL,
 "居住地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "居住地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "居住地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "居住地-行政区划代码" varchar (12) DEFAULT NULL,
 "居住地-详细地址" varchar (200) DEFAULT NULL,
 "户籍地-邮政编码" varchar (6) DEFAULT NULL,
 "户籍地-门牌号码" varchar (100) DEFAULT NULL,
 "户籍地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "户籍地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "户籍地-居委名称" varchar (32) DEFAULT NULL,
 "户籍地-居委代码" varchar (12) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "户籍地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "户籍地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "户籍地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "户籍地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "户籍地-行政区划代码" varchar (12) DEFAULT NULL,
 "户籍地-详细地址" varchar (200) DEFAULT NULL,
 "工作单位名称" varchar (70) DEFAULT NULL,
 "联系电话号码" varchar (20) DEFAULT NULL,
 "手机号码" varchar (20) DEFAULT NULL,
 "医疗费用支付方式其他" varchar (100) DEFAULT NULL,
 "医疗费用支付方式名称" varchar (100) DEFAULT NULL,
 "医疗费用支付方式代码" varchar (30) DEFAULT NULL,
 "婚姻状况名称" varchar (50) DEFAULT NULL,
 "婚姻状况代码" varchar (2) DEFAULT NULL,
 "学历名称" varchar (50) DEFAULT NULL,
 "学历代码" varchar (5) DEFAULT NULL,
 "职业类别名称" varchar (60) DEFAULT NULL,
 "职业类别代码" varchar (4) DEFAULT NULL,
 "民族名称" varchar (50) DEFAULT NULL,
 "民族代码" varchar (2) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "管理卡标识号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "饮酒情况名称" varchar (50) DEFAULT NULL,
 "体育锻炼代码" varchar (2) DEFAULT NULL,
 "体育锻炼名称" varchar (50) DEFAULT NULL,
 "饮食习惯代码" varchar (2) DEFAULT NULL,
 "饮食习惯名称" varchar (50) DEFAULT NULL,
 "心理状况代码" varchar (2) DEFAULT NULL,
 "心理状况名称" varchar (50) DEFAULT NULL,
 "身高(cm)" decimal (4,
 1) DEFAULT NULL,
 "体重(kg)" decimal (4,
 1) DEFAULT NULL,
 "体质指数" decimal (4,
 2) DEFAULT NULL,
 "腰围(cm)" decimal (4,
 1) DEFAULT NULL,
 "臀围(cm)" decimal (4,
 1) DEFAULT NULL,
 "腰臀围比" decimal (2,
 1) DEFAULT NULL,
 "心率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 CONSTRAINT "(高血压)患者管理卡"_"管理卡标识号"_"医疗机构代码"_PK PRIMARY KEY ("管理卡标识号",
 "医疗机构代码")
);


COMMENT ON TABLE "一般护理记录" IS '一般患者护理记录，包括护理操作、护理观察项目、各类护理指标结果信息';
COMMENT ON COLUMN "一般护理记录"."隔离标志" IS '标识对患者是否采取隔离措施的标志';
COMMENT ON COLUMN "一般护理记录"."隔离种类名称" IS '对患者采取的隔离种类(如消化道隔离、呼吸道隔离、接触隔离等)名称';
COMMENT ON COLUMN "一般护理记录"."护士工号" IS '护理护士的工号';
COMMENT ON COLUMN "一般护理记录"."护士姓名" IS '护理护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "一般护理记录"."签名时间" IS '护理护士在护理记录上完成签名的公元纪年和日期的完整描述';
COMMENT ON COLUMN "一般护理记录"."护理等级代码" IS '护理级别的分类在特定编码体系中的代码';
COMMENT ON COLUMN "一般护理记录"."护理等级名称" IS '护理级别的分类名称';
COMMENT ON COLUMN "一般护理记录"."护理类型代码" IS '护理类型的分类在特定编码体系中的代码';
COMMENT ON COLUMN "一般护理记录"."护理类型名称" IS '护理类型的分类名称';
COMMENT ON COLUMN "一般护理记录"."导管护理描述" IS '对患者进行导管护理的详细描述';
COMMENT ON COLUMN "一般护理记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "一般护理记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "一般护理记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "一般护理记录"."经皮胆红素测定" IS '经皮胆红素测量结果，计量单位umol/L';
COMMENT ON COLUMN "一般护理记录"."24小时出量" IS '患者24小时出量，计量单位为ml';
COMMENT ON COLUMN "一般护理记录"."24小时入量" IS '患者24小时入量，计量单位为ml';
COMMENT ON COLUMN "一般护理记录"."疼痛评分" IS '患者疼痛程度的评分结果描述';
COMMENT ON COLUMN "一般护理记录"."心率(次/min)" IS '心脏搏动频率的测量值,计量单位为次/min';
COMMENT ON COLUMN "一般护理记录"."护理操作结果" IS '护理操作结果的详细描述';
COMMENT ON COLUMN "一般护理记录"."护理操作项目类目名称" IS '多个护理操作项目的名称';
COMMENT ON COLUMN "一般护理记录"."护理操作名称" IS '进行护理操作的具体名称';
COMMENT ON COLUMN "一般护理记录"."护理观察结果" IS '对护理观察项目结果的详细描述';
COMMENT ON COLUMN "一般护理记录"."护理观察项目名称" IS '护理观察项目的名称，如患者神志状态、饮食情况，皮肤情况、氧疗情况、排尿排便情况，流量、出量、人量等等，根据护理内容的不同选择不同的观察项目名称';
COMMENT ON COLUMN "一般护理记录"."安全护理名称" IS '安全护理类别名称';
COMMENT ON COLUMN "一般护理记录"."安全护理代码" IS '安全护理类别在特定编码体系中的代码';
COMMENT ON COLUMN "一般护理记录"."心理护理名称" IS '心理护理类别(如根据病人心理状况施行心理护理、家属心理支持等)名称';
COMMENT ON COLUMN "一般护理记录"."心理护理代码" IS '心理护理类别(如根据病人心理状况施行心理护理、家属心理支持等)在标准分类系统中的代码';
COMMENT ON COLUMN "一般护理记录"."营养护理描述" IS '对患者进行营养护理的详细描述';
COMMENT ON COLUMN "一般护理记录"."皮肤护理描述" IS '对患者进行皮肤护理的详细描述';
COMMENT ON COLUMN "一般护理记录"."体位护理描述" IS '对患者进行体位护理的详细描述';
COMMENT ON COLUMN "一般护理记录"."气管护理名称" IS '气管护理类别名称';
COMMENT ON COLUMN "一般护理记录"."气管护理代码" IS '气管护理类别在特定编码体系中的代码';
COMMENT ON COLUMN "一般护理记录"."隔离种类代码" IS '对患者采取的隔离种类(如消化道隔离、呼吸道隔离、接触隔离等)在特定编码体系中的代码';
COMMENT ON COLUMN "一般护理记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "一般护理记录"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "一般护理记录"."一般护理记录流水号" IS '按照某一特定编码规则赋予一般护理记录的顺序号';
COMMENT ON COLUMN "一般护理记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "一般护理记录"."就诊次数" IS '对于门(急)诊患者，此处表示患者门(急)诊的累计次数；对于住院患者，此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "一般护理记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "一般护理记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "一般护理记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "一般护理记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "一般护理记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "一般护理记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "一般护理记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "一般护理记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "一般护理记录"."疾病诊断代码" IS '疾病诊断在特定编码体系中的代码';
COMMENT ON COLUMN "一般护理记录"."疾病诊断名称" IS '疾病诊断在特定编码体系中的名称';
COMMENT ON COLUMN "一般护理记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "一般护理记录"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "一般护理记录"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "一般护理记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "一般护理记录"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "一般护理记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "一般护理记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "一般护理记录"."过敏史" IS '患者既往发生过敏情况的详细描述';
COMMENT ON COLUMN "一般护理记录"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "一般护理记录"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "一般护理记录"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "一般护理记录"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "一般护理记录"."呼吸频率(次/min)" IS '受检者单位时间内呼吸的次数，计量单位为次/min';
COMMENT ON COLUMN "一般护理记录"."脉率(次/min)" IS '每分钟脉搏的次数测量值，计量单位为次/min';
COMMENT ON COLUMN "一般护理记录"."血氧饱和度(％)" IS '脉搏血氧饱和度的测量值,计量单位为%';
COMMENT ON COLUMN "一般护理记录"."足背动脉搏动标志" IS '标识个体是否存在足背动脉搏动的标志';
COMMENT ON COLUMN "一般护理记录"."饮食情况代码" IS '个体饮食情况所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "一般护理记录"."饮食情况名称" IS '个体饮食情况所属类别的标准名称，如良好、一般、较差等';
COMMENT ON COLUMN "一般护理记录"."饮食指导代码" IS '饮食指导类别(如普通饮食、软食、半流食、流食、禁食等)在特定编码体系中的代码';
COMMENT ON COLUMN "一般护理记录"."饮食指导名称" IS '饮食指导类别(如普通饮食、软食、半流食、流食、禁食等)名称';
COMMENT ON COLUMN "一般护理记录"."简要病情" IS '对病人简要病情的描述';
COMMENT ON COLUMN "一般护理记录"."发出手术安全核对表标志" IS '标识是否发出手术安全核对表';
COMMENT ON COLUMN "一般护理记录"."收回手术安全核对表标志" IS '标识是否收回手术安全核对表';
COMMENT ON COLUMN "一般护理记录"."发出手术风险评估表标志" IS '标识是否发出手术风险评估表';
COMMENT ON COLUMN "一般护理记录"."收回手术风险评估表标志" IS '标识是否收回手术风险评估表';
COMMENT ON COLUMN "一般护理记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
CREATE TABLE IF NOT EXISTS "一般护理记录" (
"隔离标志" varchar (1) DEFAULT NULL,
 "隔离种类名称" varchar (200) DEFAULT NULL,
 "护士工号" varchar (20) DEFAULT NULL,
 "护士姓名" varchar (50) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "护理等级代码" varchar (1) DEFAULT NULL,
 "护理等级名称" varchar (20) DEFAULT NULL,
 "护理类型代码" varchar (1) DEFAULT NULL,
 "护理类型名称" varchar (20) DEFAULT NULL,
 "导管护理描述" text,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "经皮胆红素测定" varchar (32) DEFAULT NULL,
 "24小时出量" varchar (32) DEFAULT NULL,
 "24小时入量" varchar (32) DEFAULT NULL,
 "疼痛评分" varchar (32) DEFAULT NULL,
 "心率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "护理操作结果" text,
 "护理操作项目类目名称" varchar (100) DEFAULT NULL,
 "护理操作名称" varchar (100) DEFAULT NULL,
 "护理观察结果" text,
 "护理观察项目名称" varchar (200) DEFAULT NULL,
 "安全护理名称" varchar (200) DEFAULT NULL,
 "安全护理代码" varchar (1) DEFAULT NULL,
 "心理护理名称" varchar (200) DEFAULT NULL,
 "心理护理代码" varchar (1) DEFAULT NULL,
 "营养护理描述" varchar (100) DEFAULT NULL,
 "皮肤护理描述" varchar (50) DEFAULT NULL,
 "体位护理描述" varchar (30) DEFAULT NULL,
 "气管护理名称" varchar (100) DEFAULT NULL,
 "气管护理代码" varchar (3) DEFAULT NULL,
 "隔离种类代码" varchar (1) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "一般护理记录流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "就诊次数" decimal (5,
 0) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "疾病诊断名称" varchar (512) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "过敏史" text,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "呼吸频率(次/min)" decimal (3,
 0) DEFAULT NULL,
 "脉率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "血氧饱和度(％)" decimal (4,
 0) DEFAULT NULL,
 "足背动脉搏动标志" varchar (1) DEFAULT NULL,
 "饮食情况代码" varchar (2) DEFAULT NULL,
 "饮食情况名称" varchar (50) DEFAULT NULL,
 "饮食指导代码" varchar (2) DEFAULT NULL,
 "饮食指导名称" varchar (100) DEFAULT NULL,
 "简要病情" text,
 "发出手术安全核对表标志" varchar (1) DEFAULT NULL,
 "收回手术安全核对表标志" varchar (1) DEFAULT NULL,
 "发出手术风险评估表标志" varchar (1) DEFAULT NULL,
 "收回手术风险评估表标志" varchar (1) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 CONSTRAINT "一般护理记录"_"医疗机构代码"_"一般护理记录流水号"_PK PRIMARY KEY ("医疗机构代码",
 "一般护理记录流水号")
);


COMMENT ON TABLE "不良事件_严重不良事件报告" IS '临床试验过程中发生的严重不良事件记录，包括不良事件类别、结果等信息';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."报告人职务/职称代码" IS '报告人的职务/职称在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件报告人姓名" IS '报告人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件报告单位名称" IS '报告机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."SAE发生及处理的详细情况" IS '不良事件事件发生时采取的处理方式及结果的详细描述';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."SAE报道情况(国外)名称" IS '不良事件国外报道情况(如有、无、不详等)在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."SAE报道情况(国外)代码" IS '不良事件国外报道情况(如有、无、不详等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."SAE与实验药的关系名称" IS '不良事件情况与实验药的关系(如肯定有关、肯定无关、可能有关等)在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."SAE与实验药的关系代码" IS '不良事件情况与实验药的关系(如肯定有关、肯定无关、可能有关等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."SAE转归代码" IS '严重不良事件转归(如症状消失、症状持续)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."对试验用药采取的措施名称" IS '对试验用药采取的措施(如继续用药、减少剂量等)在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."对试验用药采取的措施代码" IS '对试验用药采取的措施(如继续用药、减少剂量等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."研究者获知SAE时间" IS '研究者获知严重不良事件当时的公元纪年和日期的完整描述';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."SAE发生时间" IS '严重不良事件发生当时的公元纪年和日期的完整描述';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."死亡时间" IS '患者死亡当时的公元纪年和日期的完整描述';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."SAE情况名称" IS '严重不良事件情况(如死亡、导致住院、伤残等)在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."SAE情况代码" IS '严重不良事件情况(如死亡、导致住院、伤残等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."SAE的医学术语(诊断)" IS '由医师根据患者就诊时的情况，综合分析所作出的SAE报告中所治疗的西医疾病西医诊断机构内名称';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."用法用量" IS '对治疗合并疾病所用药物的具体服用方法及用量的详细描述';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."合并疾病治疗药物" IS '合并疾病治疗药品在机构内编码体系中的名称的组合';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."合并疾病" IS '由医师根据患者就诊时的情况，综合分析所作出的西医诊断机构内名称的组合';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."合并疾病及治疗标志" IS '标识患者是否存在其他疾病和其他治疗的标志';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."身高(cm)" IS '个体身高的测量值，计量单位为cm';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."出生日期" IS '患者出生当日的公元纪年日期';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."受试者姓名拼音缩写" IS '受试者在公安户籍管理部门正式登记注册的姓氏和名称拼音缩写';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件临床实验适应症" IS '适应证的详细描述';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件临床研究分类名称" IS '临床研究分类(如临床Ⅰ期、临床Ⅱ期、生物等效性实验、临床验证等)在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件临床研究分类代码" IS '临床研究分类(如临床Ⅰ期、临床Ⅱ期、生物等效性实验、临床验证等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件药品分类名称" IS '不良事件药品类别在特定编码体系中的名称，如中药、化学药、生物制剂等';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件药品分类代码" IS '不良事件药品类别(中药、化学药、生物制剂等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件药品注册剂型" IS '药物注册剂型的名称，如颗粒剂、片剂、丸剂、胶囊剂等';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件药品注册分类" IS '不良事件药品注册时的分类名称';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件试验用药英文商品名称" IS '不良事件试验药物商品名称的英文';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件试验用药英文名称" IS '不良事件试验药品的英文名称';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件试验用药中文商品名称" IS '不良事件试验药物商品名称的中文';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件试验用药中文名称" IS '不良事件试验药品的中文名称';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."工作单位联系电话" IS '工作单位联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件报告单位联系电话" IS '报告机构联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."申报单位名称" IS '申报单位的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."医疗机构组织机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."医疗机构组织机构代码" IS '医疗机构的组织机构代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."报告日期" IS '本次不良事件报告当日的公元纪年日期';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件报告类型名称" IS '不良事件报告类型(如首次报告、随访报告、总结报告、跟踪报告等)在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件报告类型代码" IS '不良事件报告类型(如首次报告、随访报告、总结报告、跟踪报告等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."严重不良事件编号" IS '严重不良事件编号';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."新药临床研究批准文号" IS '新药进行临床研究时的批准文号';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."严重不良事件报告编号" IS '按照某一特定编码规则赋予严重不良事件报告的顺序号，是严重不良事件报告的唯一标识';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."上传机构名称" IS '上传机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."上传机构代码" IS '上传机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."不良事件报告人职业代码" IS '报告人的职业在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "不良事件_严重不良事件报告"."文本内容" IS '文本详细内容';
CREATE TABLE IF NOT EXISTS "不良事件_严重不良事件报告" (
"报告人职务/职称代码" varchar (100) DEFAULT NULL,
 "不良事件报告人姓名" varchar (50) DEFAULT NULL,
 "不良事件报告单位名称" varchar (100) DEFAULT NULL,
 "SAE发生及处理的详细情况" varchar (1000) DEFAULT NULL,
 "SAE报道情况(国外)名称" varchar (2) DEFAULT NULL,
 "SAE报道情况(国外)代码" varchar (2) DEFAULT NULL,
 "SAE与实验药的关系名称" varchar (20) DEFAULT NULL,
 "SAE与实验药的关系代码" varchar (2) DEFAULT NULL,
 "SAE转归代码" varchar (2) DEFAULT NULL,
 "对试验用药采取的措施名称" varchar (20) DEFAULT NULL,
 "对试验用药采取的措施代码" varchar (2) DEFAULT NULL,
 "研究者获知SAE时间" timestamp DEFAULT NULL,
 "SAE发生时间" timestamp DEFAULT NULL,
 "死亡时间" timestamp DEFAULT NULL,
 "SAE情况名称" varchar (20) DEFAULT NULL,
 "SAE情况代码" varchar (2) DEFAULT NULL,
 "SAE的医学术语(诊断)" varchar (100) DEFAULT NULL,
 "用法用量" varchar (100) DEFAULT NULL,
 "合并疾病治疗药物" varchar (100) DEFAULT NULL,
 "合并疾病" varchar (100) DEFAULT NULL,
 "合并疾病及治疗标志" varchar (1) DEFAULT NULL,
 "身高(cm)" decimal (4,
 1) DEFAULT NULL,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "受试者姓名拼音缩写" varchar (50) DEFAULT NULL,
 "不良事件临床实验适应症" varchar (50) DEFAULT NULL,
 "不良事件临床研究分类名称" varchar (50) DEFAULT NULL,
 "不良事件临床研究分类代码" varchar (2) DEFAULT NULL,
 "不良事件药品分类名称" varchar (20) DEFAULT NULL,
 "不良事件药品分类代码" varchar (50) DEFAULT NULL,
 "不良事件药品注册剂型" varchar (100) DEFAULT NULL,
 "不良事件药品注册分类" varchar (100) DEFAULT NULL,
 "不良事件试验用药英文商品名称" varchar (100) DEFAULT NULL,
 "不良事件试验用药英文名称" varchar (100) DEFAULT NULL,
 "不良事件试验用药中文商品名称" varchar (100) DEFAULT NULL,
 "不良事件试验用药中文名称" varchar (100) DEFAULT NULL,
 "工作单位联系电话" varchar (20) DEFAULT NULL,
 "不良事件报告单位联系电话" varchar (20) DEFAULT NULL,
 "申报单位名称" varchar (100) DEFAULT NULL,
 "医疗机构组织机构名称" varchar (70) DEFAULT NULL,
 "医疗机构组织机构代码" varchar (50) DEFAULT NULL,
 "报告日期" date DEFAULT NULL,
 "不良事件报告类型名称" varchar (10) DEFAULT NULL,
 "不良事件报告类型代码" varchar (2) DEFAULT NULL,
 "严重不良事件编号" varchar (50) DEFAULT NULL,
 "新药临床研究批准文号" varchar (50) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "严重不良事件报告编号" varchar (32) NOT NULL,
 "上传机构名称" varchar (70) DEFAULT NULL,
 "上传机构代码" varchar (50) NOT NULL,
 "不良事件报告人职业代码" varchar (2) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "文本内容" text,
 CONSTRAINT "不良事件_严重不良事件报告"_"严重不良事件报告编号"_"上传机构代码"_PK PRIMARY KEY ("严重不良事件报告编号",
 "上传机构代码")
);


COMMENT ON TABLE "不良事件_药品不良事件" IS '药物治疗过程中发生的不良事件记录，包括药品信息、不良事件结果等';
COMMENT ON COLUMN "不良事件_药品不良事件"."有后遗症标志" IS '标识患者本次不良事件发生时是否有后遗症的标志';
COMMENT ON COLUMN "不良事件_药品不良事件"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "不良事件_药品不良事件"."文本内容" IS '文本详细内容';
COMMENT ON COLUMN "不良事件_药品不良事件"."备注" IS '其他中药内容的详细描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."报告日期" IS '报告不良事件当时的公元纪年日期的完整描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."生产企业信息来源" IS '生产企业信息来源';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告单位联系电话" IS '报告机构联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告单位名称" IS '报告机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告单位联系人" IS '报告机构联系人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告人邮箱" IS '报告人的电子邮箱名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告人联系电话" IS '报告人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告人职业代码" IS '报告人的职业务在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告人姓名" IS '报告人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告单位评价" IS '报告机构关于本次不良事件的评价详细描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告人评价" IS '报告人关于本次不良事件的评价详细描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."对原患病的影响" IS '药品不良事件发生后对患者病情变化的详细描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."再次使用药品后出现同样的反应事件标志" IS '患者再次使用药品后是否出现同样的反应事件的标志';
COMMENT ON COLUMN "不良事件_药品不良事件"."停药或减量后反应状态" IS '停药/减量后不良反应的状态(如消失、减轻、无变化等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."死亡时间" IS '患者死亡当时的公元纪年和日期的完整描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."直接死因" IS '患者死亡直接原因的详细描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."死亡标志" IS '本次不良反应事件中患者是否死亡的标志';
COMMENT ON COLUMN "不良事件_药品不良事件"."后遗症表现" IS '对个体出现后遗症症状的详细描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件结果名称" IS '不良事件转归(如痊愈、好转、未好转等)在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件结果代码" IS '不良事件转归(如痊愈、好转、未好转等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件过程简要描述" IS '不良事件过程简要描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."用药原因" IS '患者用药的原因的详细描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."用药停止时间" IS '结束使用药物时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."用药开始时间" IS '开始使用药物时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."药物使用频率名称" IS '单位时间内药物使用次数在机构内编码体系中的名称，本处指每日的使用次数';
COMMENT ON COLUMN "不良事件_药品不良事件"."药物使用剂量单位" IS '药物使用次剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "不良事件_药品不良事件"."药物使用次剂量" IS '单次使用药物的剂量，按剂量单位计';
COMMENT ON COLUMN "不良事件_药品不良事件"."用药途径名称" IS '药物的给药途径在特定编码体系中的名称，如口服、静滴、喷喉等';
COMMENT ON COLUMN "不良事件_药品不良事件"."用药途径代码" IS '药物使用途径(如口服、静滴、喷喉等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."药品采购码" IS '参见附件《药品编码-YPID5》';
COMMENT ON COLUMN "不良事件_药品不良事件"."药品生产批号" IS '按照某一特定编码规则赋予药物生产批号的唯一标志';
COMMENT ON COLUMN "不良事件_药品不良事件"."产品生产厂家" IS '药品生产企业在工商局注册、审批通过后的企业名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."产品通用名" IS '药物通用名指中国药品通用名称。是同一种成分或相同配方组成的药品在中国境内的通用名称，具有强制性和约束性。';
COMMENT ON COLUMN "不良事件_药品不良事件"."商品名" IS '药品商品名称的中文';
COMMENT ON COLUMN "不良事件_药品不良事件"."批准文号" IS '针对国产药，药品批准文号是药品监督管理部门对特定生产企业按法定标准、生产工艺和生产条件对某一药品的法律认可凭证，每一个生产企业的每一个品种都有一个特定的批准文号';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良反应药品分类名称" IS '不良反应药品分类(如怀疑药品、并用药品)在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良反应药品分类代码" IS '不良反应药品分类(如怀疑药品、并用药品)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良反应事件发生时间" IS '不良反应事件发生当时的公元纪年和日期的完整描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良反应事件名称" IS '不良反应事件名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件患者过敏史描述" IS '患者既往发生过敏情况的详细描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件患者其他既往史描述" IS '既往健康状况及重要相关病史的描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件患者既往史代码" IS '既往史类别(如吸烟史、肝病史、过敏史等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件患者家族药品不良反应" IS '患者家族的药品不良反应(包括：药物不良反应的表现、相关处理及结局)的详细描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."患者家族药品不良反应标志" IS '标识患者家族对某种药品是否存在不良反应的标志';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件患者既往药品不良反应" IS '对可能影响患者诊治的、严重的既往药品不良反应(包括：药物不良反应的表现、相关处理及结局)的详细描述';
COMMENT ON COLUMN "不良事件_药品不良事件"."患者既往药品不良反应标志" IS '标识患者对既往药品是否存在不良反应的标志';
COMMENT ON COLUMN "不良事件_药品不良事件"."医疗机构组织机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "不良事件_药品不良事件"."医疗机构组织机构代码" IS '医疗机构的组织机构代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."个人联系电话号码" IS '患者本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "不良事件_药品不良事件"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "不良事件_药品不良事件"."民族名称" IS '所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件患者年龄" IS '个体从出生当日公元纪年日起到计算当日止生存的时间长度，按计量单位计算';
COMMENT ON COLUMN "不良事件_药品不良事件"."出生日期" IS '患者出生当日的公元纪年日期';
COMMENT ON COLUMN "不良事件_药品不良事件"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "不良事件_药品不良事件"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "不良事件_药品不良事件"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件患者原疾病诊断名称" IS '由医师根据患者就诊时的情况，综合分析所作出的报告中所治疗的西医疾病西医诊断机构内名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件患者原疾病诊断代码" IS '按照机构内编码规则赋予报告中所治疗的西医疾病西医疾病的唯一标识';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告单位类别名称" IS '不良事件报告单位类别在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告单位类别代码" IS '不良事件报告单位类别在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告类型名称" IS '不良事件报告类型(如首次报告、随访报告、总结报告、跟踪报告等)在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告类型代码" IS '不良事件报告类型(如首次报告、随访报告、总结报告、跟踪报告等)在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告类别名称" IS '不良事件报告类别在特定编码体系中的名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告类别代码" IS '不良事件报告类别在特定编码体系中的代码';
COMMENT ON COLUMN "不良事件_药品不良事件"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "不良事件_药品不良事件"."不良事件报告编号" IS '按照某一特定编码规则赋予药品不良事件报告的顺序号，是药品不良事件报告的唯一标识';
COMMENT ON COLUMN "不良事件_药品不良事件"."上传机构名称" IS '上传机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "不良事件_药品不良事件"."上传机构代码" IS '上传机构在国家直报系统中的 12 位编码（如： 520000000001）';
CREATE TABLE IF NOT EXISTS "不良事件_药品不良事件" (
"有后遗症标志" varchar (1) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "文本内容" text,
 "备注" varchar (1000) DEFAULT NULL,
 "报告日期" date DEFAULT NULL,
 "生产企业信息来源" varchar (2) DEFAULT NULL,
 "不良事件报告单位联系电话" varchar (20) DEFAULT NULL,
 "不良事件报告单位名称" varchar (100) DEFAULT NULL,
 "不良事件报告单位联系人" varchar (100) DEFAULT NULL,
 "不良事件报告人邮箱" varchar (50) DEFAULT NULL,
 "不良事件报告人联系电话" varchar (20) DEFAULT NULL,
 "不良事件报告人职业代码" varchar (2) DEFAULT NULL,
 "不良事件报告人姓名" varchar (50) DEFAULT NULL,
 "不良事件报告单位评价" text,
 "不良事件报告人评价" text,
 "对原患病的影响" text,
 "再次使用药品后出现同样的反应事件标志" varchar (1) DEFAULT NULL,
 "停药或减量后反应状态" varchar (2) DEFAULT NULL,
 "死亡时间" timestamp DEFAULT NULL,
 "直接死因" varchar (1000) DEFAULT NULL,
 "死亡标志" varchar (1) DEFAULT NULL,
 "后遗症表现" varchar (100) DEFAULT NULL,
 "不良事件结果名称" varchar (10) DEFAULT NULL,
 "不良事件结果代码" varchar (2) DEFAULT NULL,
 "不良事件过程简要描述" text,
 "用药原因" text,
 "用药停止时间" timestamp DEFAULT NULL,
 "用药开始时间" timestamp DEFAULT NULL,
 "药物使用频率名称" varchar (20) DEFAULT NULL,
 "药物使用剂量单位" varchar (6) DEFAULT NULL,
 "药物使用次剂量" decimal (4,
 2) DEFAULT NULL,
 "用药途径名称" varchar (30) DEFAULT NULL,
 "用药途径代码" varchar (4) DEFAULT NULL,
 "药品采购码" varchar (30) DEFAULT NULL,
 "药品生产批号" varchar (50) DEFAULT NULL,
 "产品生产厂家" varchar (100) DEFAULT NULL,
 "产品通用名" varchar (150) DEFAULT NULL,
 "商品名" varchar (200) DEFAULT NULL,
 "批准文号" varchar (100) DEFAULT NULL,
 "不良反应药品分类名称" varchar (100) DEFAULT NULL,
 "不良反应药品分类代码" varchar (2) DEFAULT NULL,
 "不良反应事件发生时间" timestamp DEFAULT NULL,
 "不良反应事件名称" varchar (100) DEFAULT NULL,
 "不良事件患者过敏史描述" varchar (100) DEFAULT NULL,
 "不良事件患者其他既往史描述" varchar (100) DEFAULT NULL,
 "不良事件患者既往史代码" varchar (30) DEFAULT NULL,
 "不良事件患者家族药品不良反应" varchar (100) DEFAULT NULL,
 "患者家族药品不良反应标志" varchar (1) DEFAULT NULL,
 "不良事件患者既往药品不良反应" varchar (100) DEFAULT NULL,
 "患者既往药品不良反应标志" varchar (1) DEFAULT NULL,
 "医疗机构组织机构名称" varchar (70) DEFAULT NULL,
 "医疗机构组织机构代码" varchar (50) DEFAULT NULL,
 "个人联系电话号码" varchar (70) DEFAULT NULL,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "民族名称" varchar (50) DEFAULT NULL,
 "民族代码" varchar (2) DEFAULT NULL,
 "不良事件患者年龄" varchar (3) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "不良事件患者原疾病诊断名称" varchar (512) DEFAULT NULL,
 "不良事件患者原疾病诊断代码" varchar (64) DEFAULT NULL,
 "不良事件报告单位类别名称" varchar (10) DEFAULT NULL,
 "不良事件报告单位类别代码" varchar (2) DEFAULT NULL,
 "不良事件报告类型名称" varchar (10) DEFAULT NULL,
 "不良事件报告类型代码" varchar (2) DEFAULT NULL,
 "不良事件报告类别名称" varchar (10) DEFAULT NULL,
 "不良事件报告类别代码" varchar (2) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "不良事件报告编号" varchar (50) NOT NULL,
 "上传机构名称" varchar (70) DEFAULT NULL,
 "上传机构代码" varchar (50) NOT NULL,
 CONSTRAINT "不良事件_药品不良事件"_"不良事件报告编号"_"上传机构代码"_PK PRIMARY KEY ("不良事件报告编号",
 "上传机构代码")
);


COMMENT ON TABLE "业务量统计表" IS '医院每日住院业务统计，包括人次、收入和费用信息';
COMMENT ON COLUMN "业务量统计表"."体检人次" IS '业务发生日期的体检总人次';
COMMENT ON COLUMN "业务量统计表"."在院人数" IS '业务发生日期的在院总人次';
COMMENT ON COLUMN "业务量统计表"."结算(出院)人次" IS '业务发生日期的出院结算总人次';
COMMENT ON COLUMN "业务量统计表"."出院人次" IS '业务发生日期的出院总人次';
COMMENT ON COLUMN "业务量统计表"."入院人次" IS '业务发生日期的入院总人次';
COMMENT ON COLUMN "业务量统计表"."急诊人次" IS '业务发生日期的急诊总人次';
COMMENT ON COLUMN "业务量统计表"."门诊人次" IS '业务发生日期内的门诊总人次';
COMMENT ON COLUMN "业务量统计表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "业务量统计表"."业务日期" IS '业务交易发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "业务量统计表"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "业务量统计表"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "业务量统计表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "业务量统计表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "业务量统计表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "业务量统计表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "业务量统计表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "业务量统计表"."65岁以上老年人健康体检服务人次" IS '业务发生日期的65岁以上老人健康体检服务的总人次';
COMMENT ON COLUMN "业务量统计表"."计划免疫服务人次" IS '业务发生日期的计划免疫服务的总人次';
COMMENT ON COLUMN "业务量统计表"."儿童保健服务人次" IS '业务发生日期的儿童保健服务的总人次';
COMMENT ON COLUMN "业务量统计表"."手术及操作例数" IS '业务发生日期的手术及操作总例数';
COMMENT ON COLUMN "业务量统计表"."家庭病床建床数" IS '业务发生日期家庭病床建床的总数';
COMMENT ON COLUMN "业务量统计表"."家庭病床撤床数" IS '业务发生日期家庭病床撤床的总数';
COMMENT ON COLUMN "业务量统计表"."出观人次" IS '业务发生日期的留院观察出院人次';
CREATE TABLE IF NOT EXISTS "业务量统计表" (
"体检人次" decimal (15,
 3) DEFAULT NULL,
 "在院人数" decimal (15,
 0) DEFAULT NULL,
 "结算(出院)人次" decimal (15,
 0) DEFAULT NULL,
 "出院人次" decimal (15,
 0) DEFAULT NULL,
 "入院人次" decimal (15,
 0) DEFAULT NULL,
 "急诊人次" decimal (15,
 0) DEFAULT NULL,
 "门诊人次" decimal (15,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "业务日期" date NOT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "65岁以上老年人健康体检服务人次" decimal (15,
 0) DEFAULT NULL,
 "计划免疫服务人次" decimal (15,
 0) DEFAULT NULL,
 "儿童保健服务人次" decimal (15,
 0) DEFAULT NULL,
 "手术及操作例数" decimal (15,
 0) DEFAULT NULL,
 "家庭病床建床数" decimal (15,
 0) DEFAULT NULL,
 "家庭病床撤床数" decimal (15,
 0) DEFAULT NULL,
 "出观人次" decimal (15,
 3) DEFAULT NULL,
 CONSTRAINT "业务量统计表"_"业务日期"_"科室代码"_"医疗机构代码"_PK PRIMARY KEY ("业务日期",
 "科室代码",
 "医疗机构代码")
);


COMMENT ON TABLE "交接班记录" IS '经治医师发生变更时，针对患者病情、诊断、治疗经过信息的记录';
COMMENT ON COLUMN "交接班记录"."接班时间" IS '接入患者当时的公元纪年日期和时间的完整描述，转科填写转入时间';
COMMENT ON COLUMN "交接班记录"."交班者姓名" IS '交接记录中交出患者的医师的工号，转科记录填写转出医师工号';
COMMENT ON COLUMN "交接班记录"."交班者工号" IS '交出医师在公安户籍管理部门正式登记注册的姓氏和名称，转科记录填写转出医师姓名';
COMMENT ON COLUMN "交接班记录"."交班时间" IS '交出患者当时的公元纪年日期和时间的完整描述，转科填写转出时间';
COMMENT ON COLUMN "交接班记录"."接班诊疗计划" IS '交接班后或诊疗计划，包括具体的检査、中西医治疗措施及中医调护';
COMMENT ON COLUMN "交接班记录"."治则治法代码" IS '辩证结果采用的治则治法在特定编码体系中的代码。如有多条，用“，”加以分隔';
COMMENT ON COLUMN "交接班记录"."中医“四诊”观察结果" IS '中医“四诊”观察结果的详细描述，包括望、闻、问、切四诊内容';
COMMENT ON COLUMN "交接班记录"."目前诊断-中医症候名称" IS '目前诊断中医证候标准名称';
COMMENT ON COLUMN "交接班记录"."目前诊断-中医症候代码" IS '患者入院时按照平台编码规则赋予目前诊断中医证候的唯一标识';
COMMENT ON COLUMN "交接班记录"."目前诊断-中医病名名称" IS '患者入院时按照平台编码规则赋予目前诊断中医疾病的唯一标识';
COMMENT ON COLUMN "交接班记录"."目前诊断-中医病名代码" IS '患者入院时按照特定编码规则赋予目前诊断中医疾病的唯一标识';
COMMENT ON COLUMN "交接班记录"."目前诊断-西医诊断名称" IS '按照平台编码规则赋予西医目前诊断疾病的唯一标识';
COMMENT ON COLUMN "交接班记录"."目前诊断-西医诊断代码" IS '按照特定编码规则赋予当前诊断疾病的唯一标识';
COMMENT ON COLUMN "交接班记录"."入院诊断-中医症候名称" IS '由医师根据患者人院时的情况，综合分析所作出的标准中医证候名称';
COMMENT ON COLUMN "交接班记录"."入院诊断-中医症候代码" IS '患者入院时按照平台编码规则赋予初步诊断中医证候的唯一标识';
COMMENT ON COLUMN "交接班记录"."入院诊断-中医病名名称" IS '由医师根据患者人院时的情况，综合分析所作出的中医疾病标准名称';
COMMENT ON COLUMN "交接班记录"."入院诊断-中医病名代码" IS '患者入院时按照平台编码规则赋予初步诊断中医疾病的唯一标识';
COMMENT ON COLUMN "交接班记录"."入院诊断-西医诊断名称" IS '由医师根据患者入院时的情况，综合分析所作出的西医诊断标准名称';
COMMENT ON COLUMN "交接班记录"."入院诊断-西医诊断代码" IS '患者入院时按照平台编码规则赋予西医初步诊断疾病的唯一标识';
COMMENT ON COLUMN "交接班记录"."注意事项" IS '对可能出现问题及采取相应措施的描述';
COMMENT ON COLUMN "交接班记录"."目前情况" IS '对患者当前情况的详细描述';
COMMENT ON COLUMN "交接班记录"."诊疗过程描述" IS '对患者诊疗过程或抢救情况的详细描述';
COMMENT ON COLUMN "交接班记录"."入院情况" IS '对患者入院情况的详细描述';
COMMENT ON COLUMN "交接班记录"."主诉" IS '对患者本次疾病相关的主要症状及其持续时间的描述，一般由患者本人或监护人描述';
COMMENT ON COLUMN "交接班记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "交接班记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "交接班记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "交接班记录"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "交接班记录"."住院次数" IS '办理完整住院治疗手续的次数';
COMMENT ON COLUMN "交接班记录"."科室代码" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "交接班记录"."科室名称" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "交接班记录"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "交接班记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "交接班记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "交接班记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "交接班记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "交接班记录"."性别名称" IS '一般指患者医学生理性别，指男性或女性';
COMMENT ON COLUMN "交接班记录"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "交接班记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "交接班记录"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "交接班记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "交接班记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "交接班记录"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "交接班记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "交接班记录"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名';
COMMENT ON COLUMN "交接班记录"."交接班记录流水号" IS '按照某一特性编码规则赋予本次交接记录的唯一标识';
COMMENT ON COLUMN "交接班记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "交接班记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "交接班记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "交接班记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "交接班记录"."文本内容" IS '存入大文本的内容，最大不超过64K';
COMMENT ON COLUMN "交接班记录"."接班者姓名" IS '交接记录中接入患者的医师的工号，转科记录填写转入医师工号';
COMMENT ON COLUMN "交接班记录"."接班者工号" IS '接入医师在公安户籍管理部门正式登记注册的姓氏和名称，转科记录填写转入医师姓名';
CREATE TABLE IF NOT EXISTS "交接班记录" (
"接班时间" timestamp DEFAULT NULL,
 "交班者姓名" varchar (50) DEFAULT NULL,
 "交班者工号" varchar (20) DEFAULT NULL,
 "交班时间" timestamp DEFAULT NULL,
 "接班诊疗计划" text,
 "治则治法代码" varchar (100) DEFAULT NULL,
 "中医“四诊”观察结果" text,
 "目前诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "目前诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "目前诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "目前诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "目前诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "目前诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "入院诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "入院诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "入院诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "入院诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "入院诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "入院诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "注意事项" text,
 "目前情况" text,
 "诊疗过程描述" text,
 "入院情况" text,
 "主诉" text,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "交接班记录流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "文本内容" text,
 "接班者姓名" varchar (50) DEFAULT NULL,
 "接班者工号" varchar (20) DEFAULT NULL,
 CONSTRAINT "交接班记录"_"医疗机构代码"_"交接班记录流水号"_PK PRIMARY KEY ("医疗机构代码",
 "交接班记录流水号")
);


COMMENT ON TABLE "会诊记录" IS '患者需要会诊时，关于患者病情、诊疗过程、会诊类型、目的以及医师相关信息的记录';
COMMENT ON COLUMN "会诊记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "会诊记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "会诊记录"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "会诊记录"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "会诊记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "会诊记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "会诊记录"."住院次数" IS '办理完整住院治疗手续的次数';
COMMENT ON COLUMN "会诊记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "会诊记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "会诊记录"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "会诊记录"."科室名称" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "会诊记录"."科室代码" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "会诊记录"."申请时间" IS '患者申请会诊时的公元纪年日期时间的完整描述';
COMMENT ON COLUMN "会诊记录"."电子申请单编号" IS '按照某一特定编码规则赋予电子申请单的顺序号';
COMMENT ON COLUMN "会诊记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "会诊记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "会诊记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "会诊记录"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "会诊记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "会诊记录"."会诊记录流水号" IS '按照某一特性编码规则赋予会诊唯一标志的顺序号';
COMMENT ON COLUMN "会诊记录"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "会诊记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "会诊记录"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "会诊记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "会诊记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "会诊记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "会诊记录"."文本内容" IS '存入大文本的内容，最大不超过64K';
COMMENT ON COLUMN "会诊记录"."医嘱明细流水号" IS '医嘱表中的对应医嘱号';
COMMENT ON COLUMN "会诊记录"."会诊时间" IS '本次会诊结束时的公元纪年日期时间的完整描述';
COMMENT ON COLUMN "会诊记录"."会诊所在医疗机构名称" IS '会诊所在医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "会诊记录"."会诊所在医疗机构代码" IS '会诊所在医疗机构经《医疗机构执业许可证》登记的，在机构内编码体系填写的代码';
COMMENT ON COLUMN "会诊记录"."会诊医师所在医疗机构名称" IS '会诊医师所在医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "会诊记录"."会诊医师所在医疗机构代码" IS '会诊医师所在医疗机构经《医疗机构执业许可证》登记的，在机构内编码体系填写的代码';
COMMENT ON COLUMN "会诊记录"."会诊科室名称" IS '会诊科室在机构内编码体系中的名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "会诊记录"."会诊科室代码" IS '按照机构内编码规则赋予会诊科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "会诊记录"."会诊医师姓名" IS '会诊医师公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "会诊记录"."会诊医师工号" IS '会诊医师在原始特定编码体系中的编号';
COMMENT ON COLUMN "会诊记录"."会诊意见" IS '由会诊医师填写患者会诊时的主要处置、指导意见的详细描述';
COMMENT ON COLUMN "会诊记录"."责任医师姓名" IS '责任医师公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "会诊记录"."责任医师工号" IS '责任医师在原始特定编码体系中的编号';
COMMENT ON COLUMN "会诊记录"."会诊申请医疗机构名称" IS '申请医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "会诊记录"."会诊申请科室名称" IS '申请科室在机构内编码体系中的名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "会诊记录"."会诊申请科室代码" IS '按照机构内编码规则赋予申请科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "会诊记录"."会诊申请医师姓名" IS '会诊申请公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "会诊记录"."会诊申请医师工号" IS '会诊申请在原始特定编码体系中的编号';
COMMENT ON COLUMN "会诊记录"."会诊目的" IS '会诊申请医师就患者目前存在问题提出会诊要达到的目的';
COMMENT ON COLUMN "会诊记录"."会诊原因" IS '由会诊申请医师填写患者需会诊的主要情况的详细描述';
COMMENT ON COLUMN "会诊记录"."会诊类型" IS '会诊类型名称，如急会诊、普通会诊、院外会诊、多学科会诊、远程会诊等';
COMMENT ON COLUMN "会诊记录"."诊疗过程描述" IS '对患者诊疗过程或抢救情况的详细描述';
COMMENT ON COLUMN "会诊记录"."诊疗过程名称" IS '对患者诊疗过程或抢救情况采取措施的名称';
COMMENT ON COLUMN "会诊记录"."中医症候名称" IS '中医证候标准名称';
COMMENT ON COLUMN "会诊记录"."中医症候代码" IS '患者入院时按照平台编码规则赋予中医证候的唯一标识';
COMMENT ON COLUMN "会诊记录"."中医病名名称" IS '患者入院时按照平台编码规则赋予中医疾病的唯一标识';
COMMENT ON COLUMN "会诊记录"."中医病名代码" IS '患者入院时按照特定编码规则赋予中医疾病的唯一标识';
COMMENT ON COLUMN "会诊记录"."西医诊断名称" IS '西医诊断标准名称';
COMMENT ON COLUMN "会诊记录"."西医诊断代码" IS '按照平台编码规则赋予西医疾病的唯一标识';
COMMENT ON COLUMN "会诊记录"."治则治法代码" IS '辩证结果采用的治则治法在特定编码体系中的代码。如有多条，用“，”加以分隔';
COMMENT ON COLUMN "会诊记录"."中医“四诊”观察结果" IS '中医“四诊”观察结果的详细描述，包括望、闻、问、切四诊内容';
COMMENT ON COLUMN "会诊记录"."辅助检查结果" IS '受检者辅助检查结果的详细描述';
COMMENT ON COLUMN "会诊记录"."病历摘要" IS '对患者病情摘要的详细描述';
COMMENT ON COLUMN "会诊记录"."患者证件号码" IS '患者各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "会诊记录"."证件类型代码" IS '患者身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "会诊记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
CREATE TABLE IF NOT EXISTS "会诊记录" (
"年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "申请时间" timestamp DEFAULT NULL,
 "电子申请单编号" varchar (100) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "会诊记录流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "文本内容" text,
 "医嘱明细流水号" varchar (36) DEFAULT NULL,
 "会诊时间" timestamp DEFAULT NULL,
 "会诊所在医疗机构名称" varchar (70) DEFAULT NULL,
 "会诊所在医疗机构代码" varchar (22) DEFAULT NULL,
 "会诊医师所在医疗机构名称" varchar (200) DEFAULT NULL,
 "会诊医师所在医疗机构代码" varchar (200) DEFAULT NULL,
 "会诊科室名称" varchar (100) DEFAULT NULL,
 "会诊科室代码" varchar (20) DEFAULT NULL,
 "会诊医师姓名" varchar (50) DEFAULT NULL,
 "会诊医师工号" varchar (20) DEFAULT NULL,
 "会诊意见" text,
 "责任医师姓名" varchar (50) DEFAULT NULL,
 "责任医师工号" varchar (20) DEFAULT NULL,
 "会诊申请医疗机构名称" varchar (70) DEFAULT NULL,
 "会诊申请科室名称" varchar (100) DEFAULT NULL,
 "会诊申请科室代码" varchar (20) DEFAULT NULL,
 "会诊申请医师姓名" varchar (50) DEFAULT NULL,
 "会诊申请医师工号" varchar (20) DEFAULT NULL,
 "会诊目的" varchar (50) DEFAULT NULL,
 "会诊原因" varchar (200) DEFAULT NULL,
 "会诊类型" varchar (50) DEFAULT NULL,
 "诊疗过程描述" text,
 "诊疗过程名称" text,
 "中医症候名称" varchar (50) DEFAULT NULL,
 "中医症候代码" varchar (64) DEFAULT NULL,
 "中医病名名称" varchar (50) DEFAULT NULL,
 "中医病名代码" varchar (50) DEFAULT NULL,
 "西医诊断名称" varchar (100) DEFAULT NULL,
 "西医诊断代码" varchar (20) DEFAULT NULL,
 "治则治法代码" varchar (100) DEFAULT NULL,
 "中医“四诊”观察结果" text,
 "辅助检查结果" varchar (1000) DEFAULT NULL,
 "病历摘要" varchar (200) DEFAULT NULL,
 "患者证件号码" varchar (32) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 CONSTRAINT "会诊记录"_"会诊记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("会诊记录流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "住院业务汇总" IS '医院每日门诊业务统计，包括人次、收入和费用信息';
COMMENT ON COLUMN "住院业务汇总"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "住院业务汇总"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "住院业务汇总"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "住院业务汇总"."中草药费" IS '业务交易日期内的住院中草药费用总和';
COMMENT ON COLUMN "住院业务汇总"."中成药费" IS '业务交易日期内的住院中成药费用总和';
COMMENT ON COLUMN "住院业务汇总"."西药费" IS '业务交易日期内的住院西药费用总和';
COMMENT ON COLUMN "住院业务汇总"."输氧费" IS '业务交易日期内的住院输氧费用总和';
COMMENT ON COLUMN "住院业务汇总"."输血费" IS '业务交易日期内的住院输血费用总和';
COMMENT ON COLUMN "住院业务汇总"."透视费" IS '业务交易日期内的住院透视费用总和';
COMMENT ON COLUMN "住院业务汇总"."摄片费" IS '业务交易日期内的住院摄片费用总和';
COMMENT ON COLUMN "住院业务汇总"."化验费" IS '业务交易日期内的住院化验费用总和';
COMMENT ON COLUMN "住院业务汇总"."检查费" IS '业务交易日期内的住院检查费用总和';
COMMENT ON COLUMN "住院业务汇总"."手术材料费" IS '业务交易日期内的住院手术材料费用总和';
COMMENT ON COLUMN "住院业务汇总"."护理费" IS '业务交易日期内的住院护理费用总和';
COMMENT ON COLUMN "住院业务汇总"."治疗费" IS '业务交易日期内的住院治疗费用总和';
COMMENT ON COLUMN "住院业务汇总"."诊疗费" IS '业务交易日期内的住院诊疗费用总和';
COMMENT ON COLUMN "住院业务汇总"."住院费" IS '业务交易日期内的住院费用总和';
COMMENT ON COLUMN "住院业务汇总"."住院出院小结数" IS '业务交易日期内的住院出院小结的总数';
COMMENT ON COLUMN "住院业务汇总"."住院医嘱明细记录数" IS '业务交易日期内的住院开具的医嘱明细记录总数';
COMMENT ON COLUMN "住院业务汇总"."住院总收入" IS '业务交易日期内的住院总收入';
COMMENT ON COLUMN "住院业务汇总"."出院人数" IS '业务交易日期内的办理出院手续的总人数';
COMMENT ON COLUMN "住院业务汇总"."入院人数" IS '业务交易日期内的办理入院手续的总人数';
COMMENT ON COLUMN "住院业务汇总"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "住院业务汇总"."业务日期" IS '业务交易发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "住院业务汇总"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "住院业务汇总"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "住院业务汇总"."其它费" IS '业务交易日期内的住院除上述费用外的其它费用总和';
CREATE TABLE IF NOT EXISTS "住院业务汇总" (
"密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "中草药费" decimal (18,
 3) DEFAULT NULL,
 "中成药费" decimal (18,
 3) DEFAULT NULL,
 "西药费" decimal (18,
 3) DEFAULT NULL,
 "输氧费" decimal (18,
 3) DEFAULT NULL,
 "输血费" decimal (18,
 3) DEFAULT NULL,
 "透视费" decimal (18,
 3) DEFAULT NULL,
 "摄片费" decimal (18,
 3) DEFAULT NULL,
 "化验费" decimal (18,
 3) DEFAULT NULL,
 "检查费" decimal (18,
 3) DEFAULT NULL,
 "手术材料费" decimal (18,
 3) DEFAULT NULL,
 "护理费" decimal (18,
 3) DEFAULT NULL,
 "治疗费" decimal (18,
 3) DEFAULT NULL,
 "诊疗费" decimal (10,
 3) DEFAULT NULL,
 "住院费" decimal (18,
 3) DEFAULT NULL,
 "住院出院小结数" decimal (10,
 0) DEFAULT NULL,
 "住院医嘱明细记录数" decimal (10,
 0) DEFAULT NULL,
 "住院总收入" decimal (18,
 3) DEFAULT NULL,
 "出院人数" decimal (5,
 0) DEFAULT NULL,
 "入院人数" decimal (10,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "业务日期" date NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "其它费" decimal (18,
 3) DEFAULT NULL,
 CONSTRAINT "住院业务汇总"_"业务日期"_"医疗机构代码"_PK PRIMARY KEY ("业务日期",
 "医疗机构代码")
);


COMMENT ON TABLE "住院工作量及病床分科统计表(日报)" IS '按科室统计每日床位占用情况';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."陪客人数" IS '业务统计日内陪客人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."期内留院(在院)人数" IS '业务统计日内留院(在院)人数，即今日留院实际占用床日数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."转往他科人数" IS '业务统计日内转往他科人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."出院病人手术相关感染人数" IS '业务统计日内出院病人手术相关感染人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."出院病人医院感染人(例)数" IS '业务统计日内出院病人医院感染人(例)数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."其中：抢救成功人次数" IS '业务统计日内住院危重病人抢救成功人次';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."住院危重病人抢救人次数" IS '业务统计日内住院危重病人抢救人次';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."业务日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."平台科室代码" IS '按照特定编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."平台科室名称" IS '出院科室在特定编码体系中的名称';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."核定(编制)床位总数" IS '业务统计日内核定编制的床位总数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."实际开放床位总数" IS '业务统计日内实际开放的床位总数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."本日加床数" IS '业务统计日内实际加床的床位数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."本日空床数" IS '业务统计日内实际空出的床位数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."期初留院人数(昨日留院)" IS '业务统计日开始时的住院人数，即昨日留院人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."期内入院人数(今日入院)" IS '业务统计日内入院的人数，即今日入院人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."同科转入人数" IS '业务统计日内同科室转入的人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."他科转入人数" IS '业务统计日内其他科室转入的仁数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."出院患者占用床日数" IS '业务统计日内出院患者所占用的床日数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."期内出院人数(今日出院)" IS '业务统计日内出院的总人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."治愈出院人数" IS '业务统计日内出院人数中治愈的人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."好转出院人数" IS '业务统计日内出院人数中好转的人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."未愈出院人数" IS '业务统计日内出院人数中未愈的人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."死亡出院人数" IS '业务统计日内出院人数中死亡的人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."其他出院人数" IS '业务统计日内出院人数中除治愈、好转、未愈、死亡外，其他类型的人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."医嘱离院人数" IS '业务统计日内出院人数中医嘱离院的人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."医嘱转院人数" IS '业务统计日内出院人数中医嘱转院的人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."医嘱转社区卫生服务机构/乡镇卫生院人数" IS '业务统计日内出院人数中医嘱转社区卫生服务机构/乡镇卫生院的人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."非医嘱离院人数" IS '业务统计日内出院人数中非医嘱离院的人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."甲级病案人数" IS '业务统计日内病案质量为甲级人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."乙级病案人数" IS '业务统计日内病案质量为乙级人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."丙级病案人数" IS '业务统计日内病案质量为丙级人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."住院人员接受中医治疗人数" IS '业务统计日内住院人员接受中医治疗人数。中西医结合所有科室、其他医院中医科住院人次';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."出院者使用非药物中医技术诊疗人数" IS '业务统计日内出院者使用非药物中医技术诊疗人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."出院者使用中草药人数" IS '业务统计日内出院者使用中草药人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."出院者使用中成药人数" IS '业务统计日内出院者使用中成药人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."出院者使用中医非药物人数" IS '业务统计日内出院者使用中医非药物人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."转往同科人数" IS '业务统计日内转往同科人数';
COMMENT ON COLUMN "住院工作量及病床分科统计表(日报)"."危重病人数" IS '业务统计日内危重病人数';
CREATE TABLE IF NOT EXISTS "住院工作量及病床分科统计表(
日报)" ("陪客人数" decimal (15,
 0) DEFAULT NULL,
 "期内留院(在院)人数" decimal (15,
 0) DEFAULT NULL,
 "转往他科人数" decimal (15,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "出院病人手术相关感染人数" decimal (15,
 0) DEFAULT NULL,
 "出院病人医院感染人(例)数" decimal (15,
 0) DEFAULT NULL,
 "其中：抢救成功人次数" decimal (15,
 0) DEFAULT NULL,
 "住院危重病人抢救人次数" decimal (15,
 0) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "科室代码" varchar (20) NOT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "业务日期" date NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "平台科室代码" varchar (20) DEFAULT NULL,
 "平台科室名称" varchar (100) DEFAULT NULL,
 "核定(编制)床位总数" decimal (15,
 0) DEFAULT NULL,
 "实际开放床位总数" decimal (10,
 0) DEFAULT NULL,
 "本日加床数" decimal (15,
 0) DEFAULT NULL,
 "本日空床数" decimal (15,
 0) DEFAULT NULL,
 "期初留院人数(昨日留院)" decimal (15,
 0) DEFAULT NULL,
 "期内入院人数(今日入院)" decimal (15,
 0) DEFAULT NULL,
 "同科转入人数" decimal (15,
 0) DEFAULT NULL,
 "他科转入人数" decimal (15,
 0) DEFAULT NULL,
 "出院患者占用床日数" decimal (15,
 0) DEFAULT NULL,
 "期内出院人数(今日出院)" decimal (15,
 0) DEFAULT NULL,
 "治愈出院人数" decimal (15,
 0) DEFAULT NULL,
 "好转出院人数" decimal (15,
 0) DEFAULT NULL,
 "未愈出院人数" decimal (15,
 0) DEFAULT NULL,
 "死亡出院人数" decimal (15,
 0) DEFAULT NULL,
 "其他出院人数" decimal (15,
 0) DEFAULT NULL,
 "医嘱离院人数" decimal (15,
 0) DEFAULT NULL,
 "医嘱转院人数" decimal (15,
 0) DEFAULT NULL,
 "医嘱转社区卫生服务机构/乡镇卫生院人数" decimal (15,
 0) DEFAULT NULL,
 "非医嘱离院人数" decimal (15,
 0) DEFAULT NULL,
 "甲级病案人数" decimal (15,
 0) DEFAULT NULL,
 "乙级病案人数" decimal (15,
 0) DEFAULT NULL,
 "丙级病案人数" decimal (15,
 0) DEFAULT NULL,
 "住院人员接受中医治疗人数" decimal (15,
 0) DEFAULT NULL,
 "出院者使用非药物中医技术诊疗人数" decimal (15,
 0) DEFAULT NULL,
 "出院者使用中草药人数" decimal (15,
 0) DEFAULT NULL,
 "出院者使用中成药人数" decimal (15,
 0) DEFAULT NULL,
 "出院者使用中医非药物人数" decimal (15,
 0) DEFAULT NULL,
 "转往同科人数" decimal (15,
 0) DEFAULT NULL,
 "危重病人数" decimal (15,
 0) DEFAULT NULL,
 CONSTRAINT "住院工作量及病床分科统计表(日报)"_"医疗机构代码"_"科室代码"_"业务日期"_PK PRIMARY KEY ("医疗机构代码",
 "科室代码",
 "业务日期")
);


COMMENT ON TABLE "体格及功能检查报告表" IS '体检报告中分科结果及结论描述';
COMMENT ON COLUMN "体格及功能检查报告表"."报告时间" IS '分科报告生成当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "体格及功能检查报告表"."归档检索时间" IS '完成归档检索的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "体格及功能检查报告表"."报告医师工号" IS '报告医师在机构内编码体系中的编号';
COMMENT ON COLUMN "体格及功能检查报告表"."报告医师姓名" IS '做出报告的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "体格及功能检查报告表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "体格及功能检查报告表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "体格及功能检查报告表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "体格及功能检查报告表"."小结" IS '对同一体检科室检查项目诊断结论的描述，科室小结或者大项小结';
COMMENT ON COLUMN "体格及功能检查报告表"."影像检查标志" IS '标识是否影像学检查的标志';
COMMENT ON COLUMN "体格及功能检查报告表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "体格及功能检查报告表"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "体格及功能检查报告表"."报告流水号" IS '按照某一特定编码规则赋予体格及功能检查报告的唯一标识';
COMMENT ON COLUMN "体格及功能检查报告表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "体格及功能检查报告表"."体检流水号" IS '按照某一特定编码规则赋予体检就诊记录的唯一标识';
COMMENT ON COLUMN "体格及功能检查报告表"."科室代码" IS '体检科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)在机构内编码体系中的代码';
COMMENT ON COLUMN "体格及功能检查报告表"."科室名称" IS '体检科室在机构内编码体系中的名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科';
COMMENT ON COLUMN "体格及功能检查报告表"."检查组合名称" IS '体检系统中检查大项目在机构内编码系统中的名称，如内科检查、女外科检查，眼科、肝功十三项、血常规、空腹血糖等';
COMMENT ON COLUMN "体格及功能检查报告表"."报告标题" IS '分科小结报告的标题';
CREATE TABLE IF NOT EXISTS "体格及功能检查报告表" (
"报告时间" timestamp DEFAULT NULL,
 "归档检索时间" timestamp DEFAULT NULL,
 "报告医师工号" varchar (20) DEFAULT NULL,
 "报告医师姓名" varchar (50) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "小结" varchar (1000) DEFAULT NULL,
 "影像检查标志" varchar (1) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "报告流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "体检流水号" varchar (64) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "检查组合名称" varchar (50) DEFAULT NULL,
 "报告标题" text,
 CONSTRAINT "体格及功能检查报告表"_"医疗机构代码"_"报告流水号"_PK PRIMARY KEY ("医疗机构代码",
 "报告流水号")
);


COMMENT ON TABLE "体检就诊记录表" IS '体检套餐、分类以及体检日期的记录';
COMMENT ON COLUMN "体检就诊记录表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "体检就诊记录表"."体检套餐代码" IS '体检套餐或者组合项目在特定编码体系中的代码';
COMMENT ON COLUMN "体检就诊记录表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "体检就诊记录表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "体检就诊记录表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "体检就诊记录表"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "体检就诊记录表"."体检流水号" IS '按照某一特定编码规则赋予体检就诊记录的唯一标识';
COMMENT ON COLUMN "体检就诊记录表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "体检就诊记录表"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "体检就诊记录表"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "体检就诊记录表"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "体检就诊记录表"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "体检就诊记录表"."身份证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "体检就诊记录表"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "体检就诊记录表"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "体检就诊记录表"."年龄" IS '个体从出生当日公元纪年日起到计算当日止生存的时间长度，按计量单位计算';
COMMENT ON COLUMN "体检就诊记录表"."年龄单位" IS '年龄单位的详细描述，如岁、月、天、时，默认“岁”';
COMMENT ON COLUMN "体检就诊记录表"."患者医疗付款方式" IS '患者体检所发生费用的结算方式(如现金、支票、汇款存款、银行卡等)在特定编码体系中的代码';
COMMENT ON COLUMN "体检就诊记录表"."患者医疗付款证件号码" IS '患者单次诊疗所发生费用的支付方式对应的证件号码';
COMMENT ON COLUMN "体检就诊记录表"."体检分类代码" IS '体检目的的类别(如健康体检、入职体检、招工体检、学生体检、征兵体检等)在特定编码体系中的代码';
COMMENT ON COLUMN "体检就诊记录表"."体检套餐名称" IS '体检套餐或者组合项目在特定编码体系中对应的名称';
COMMENT ON COLUMN "体检就诊记录表"."体检时间" IS '完成体检登记时的公元纪年日期和时间的完整描述';
CREATE TABLE IF NOT EXISTS "体检就诊记录表" (
"密级" varchar (16) DEFAULT NULL,
 "体检套餐代码" varchar (50) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "体检流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "身份证件号码" varchar (32) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "年龄" decimal (3,
 0) DEFAULT NULL,
 "年龄单位" varchar (10) DEFAULT NULL,
 "患者医疗付款方式" varchar (2) DEFAULT NULL,
 "患者医疗付款证件号码" varchar (50) DEFAULT NULL,
 "体检分类代码" decimal (1,
 0) DEFAULT NULL,
 "体检套餐名称" varchar (100) DEFAULT NULL,
 "体检时间" timestamp DEFAULT NULL,
 CONSTRAINT "体检就诊记录表"_"医疗机构代码"_"体检流水号"_PK PRIMARY KEY ("医疗机构代码",
 "体检流水号")
);


COMMENT ON TABLE "入院记录" IS '病史、体格检查以及入院诊断信息';
COMMENT ON COLUMN "入院记录"."确定诊断-中医病名代码" IS '患者入院时按照特定编码规则赋予确定诊断中医疾病的唯一标识';
COMMENT ON COLUMN "入院记录"."确定诊断-西医诊断代码" IS '按照平台编码规则赋予西医确定诊断疾病的唯一标识';
COMMENT ON COLUMN "入院记录"."修正诊断时间" IS '修正诊断下达当日的公元纪年和时间的完整描述';
COMMENT ON COLUMN "入院记录"."修正诊断-中医症候名称" IS '修正诊断中医证候标准名称';
COMMENT ON COLUMN "入院记录"."修正诊断-中医症候代码" IS '患者入院时按照平台编码规则赋予修正诊断中医证候的唯一标识';
COMMENT ON COLUMN "入院记录"."修正诊断-中医病名名称" IS '患者入院时按照平台编码规则赋予修正诊断中医疾病的唯一标识';
COMMENT ON COLUMN "入院记录"."修正诊断-中医病名代码" IS '患者入院时按照特定编码规则赋予修正诊断中医疾病的唯一标识';
COMMENT ON COLUMN "入院记录"."修正诊断-西医诊断名称" IS '修正诊断西医诊断标准名称';
COMMENT ON COLUMN "入院记录"."修正诊断-西医诊断代码" IS '按照平台编码规则赋予西医修正诊断疾病的唯一标识';
COMMENT ON COLUMN "入院记录"."初步诊断时间" IS '初步诊断下达当日的公元纪年和时间的完整描述';
COMMENT ON COLUMN "入院记录"."初步诊断-中医症候名称" IS '由医师根据患者人院时的情况，综合分析所作出的标准中医证候名称';
COMMENT ON COLUMN "入院记录"."初步诊断-中医症候代码" IS '患者入院时按照平台编码规则赋予初步诊断中医证候的唯一标识';
COMMENT ON COLUMN "入院记录"."初步诊断-中医病名名称" IS '由医师根据患者人院时的情况，综合分析所作出的中医疾病标准名称';
COMMENT ON COLUMN "入院记录"."初步诊断-中医病名代码" IS '患者入院时按照平台编码规则赋予初步诊断中医疾病的唯一标识';
COMMENT ON COLUMN "入院记录"."初步诊断-西医诊断名称" IS '由医师根据患者入院时的情况，综合分析所作出的西医诊断标准名称';
COMMENT ON COLUMN "入院记录"."初步诊断-西医诊断代码" IS '患者入院时按照平台编码规则赋予西医初步诊断疾病的唯一标识';
COMMENT ON COLUMN "入院记录"."治则治法名称" IS '根据辨证结果采用的治则治法名称术语在编码体系中的名称';
COMMENT ON COLUMN "入院记录"."治则治法代码" IS '根据辨证结果采用的治则治法名称术语在编码体系中的代码';
COMMENT ON COLUMN "入院记录"."辨证分型名称" IS '中医辨证分型类别在特定编码体系中的名称，如气虚证、血热证等';
COMMENT ON COLUMN "入院记录"."辨证分型代码" IS '中医辨证分型类别在特定编码体系中的代码';
COMMENT ON COLUMN "入院记录"."中医“四诊”观察结果" IS '中医“四诊”观察结果的详细描述，包括望、闻问、切四诊内容';
COMMENT ON COLUMN "入院记录"."辅助检查结果" IS '患者辅助检查结果的详细描述';
COMMENT ON COLUMN "入院记录"."专科情况" IS '根据专科需要对患者进行专科特殊检查结果的详细描述';
COMMENT ON COLUMN "入院记录"."神经系统检查结果" IS '对受检者神经系统检查结果的详细描述';
COMMENT ON COLUMN "入院记录"."四肢检查结果" IS '对个体四肢检查详细结果的描述';
COMMENT ON COLUMN "入院记录"."脊柱检查结果" IS '受检者脊柱检查结果的详细描述';
COMMENT ON COLUMN "入院记录"."外生殖器检查结果" IS '对受检者外生殖器检查结果的详细描述';
COMMENT ON COLUMN "入院记录"."直肠肛门检查结果描述" IS '肛门指诊检查结果的详细描述';
COMMENT ON COLUMN "入院记录"."腹部检查结果" IS '对受检者腹部(肝脾等)检查结果的详细描述，包括视触叩听的检查结果';
COMMENT ON COLUMN "入院记录"."胸部检查结果" IS '对受检者胸部(胸廓、肺部、心脏、血管)检查结果的详细描述，包括视触叩听的检查结果';
COMMENT ON COLUMN "入院记录"."颈部检查结果" IS '对受检者颈部检查结果的详细描述';
COMMENT ON COLUMN "入院记录"."头部及其器官检查结果" IS '对受检者头部及其器官检查结果的详细描述';
COMMENT ON COLUMN "入院记录"."全身浅表淋巴结检查结果" IS '对受检者淋巴结检查结果的详细描述';
COMMENT ON COLUMN "入院记录"."皮肤和黏膜检查结果" IS '对受检者皮肤和黏膜检查结果的详细描述';
COMMENT ON COLUMN "入院记录"."一般状况检查结果" IS '对受检者一般状况检查结果的详细描述，包括其发育状况、营养状况、体味、步态、面容与表情、意识，检查能否合作等等';
COMMENT ON COLUMN "入院记录"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "入院记录"."身高(cm)" IS '个体身高的测量值，计量单位为cm';
COMMENT ON COLUMN "入院记录"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "入院记录"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "入院记录"."呼吸频率(次/min)" IS '单位时间内呼吸的次数,计壁单位为次/min';
COMMENT ON COLUMN "入院记录"."脉率(次/min)" IS '每分钟脉搏的次数测量值，计量单位为次/min';
COMMENT ON COLUMN "入院记录"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "入院记录"."家族史" IS '患者3代以内有血缘关系的家族成员中所患遗传疾病史的描述';
COMMENT ON COLUMN "入院记录"."月经史" IS '对患者月经史的详细描述';
COMMENT ON COLUMN "入院记录"."婚育史" IS '对患者婚育史的详细描述';
COMMENT ON COLUMN "入院记录"."个人史" IS '患者个人生活习惯及有无烟、酒、药物等嗜好，职业与工作条件是否有工业毒物、粉尘、放射性物质接触史，有无冶游史的描述';
COMMENT ON COLUMN "入院记录"."过敏史" IS '患者既往发生过敏情况的详细描述';
COMMENT ON COLUMN "入院记录"."输血史" IS '对患者既往输血史的详细描述';
COMMENT ON COLUMN "入院记录"."手术史" IS '对患者既往接受手术/操作经历的详细描述';
COMMENT ON COLUMN "入院记录"."预防接种史" IS '患者预防接种情况的详细描述';
COMMENT ON COLUMN "入院记录"."传染病史" IS '患者既往所患各种急性或慢性传染性疾病名称的详细描述';
COMMENT ON COLUMN "入院记录"."传染病标志" IS '标识患者是否具有传染性的标志';
COMMENT ON COLUMN "入院记录"."疾病史(含外伤)" IS '对个体既往健康状况和疾病(含外伤)的详细描述';
COMMENT ON COLUMN "入院记录"."一般健康状况标志" IS '标识是否有一般健康状况标志';
COMMENT ON COLUMN "入院记录"."现病史" IS '对患者当前所患疾病情况的详细描述';
COMMENT ON COLUMN "入院记录"."主诉" IS '对患者本次疾病相关的主要症状及其持续时间的描述，一般由患者本人或监护人描述';
COMMENT ON COLUMN "入院记录"."陈述内容可靠标志" IS '标识陈述内容是否可信的标志';
COMMENT ON COLUMN "入院记录"."病史陈述者地址" IS '患者病史陈述人的现住地址';
COMMENT ON COLUMN "入院记录"."病史陈述者联系电话" IS '患者病史陈述人的联系电话';
COMMENT ON COLUMN "入院记录"."陈述者与患者的关系名称" IS '患者病史陈述人与患者的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的名称';
COMMENT ON COLUMN "入院记录"."陈述者与患者的关系代码" IS '患者病史陈述人与患者的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院记录"."病史陈述者姓名" IS '患者病史的陈述人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院记录"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "入院记录"."职业类别名称" IS '本人从事职业所属类别(如国家公务员、专业技术人员、职员、工人等)在特定编码体系中的名称';
COMMENT ON COLUMN "入院记录"."职业类别代码" IS '本人从事职业所属类别(如国家公务员、专业技术人员、职员、工人等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院记录"."现住详细地址" IS '个人现住地址的详细地址';
COMMENT ON COLUMN "入院记录"."现住址-门牌号码" IS '个人现住地址的门牌号码';
COMMENT ON COLUMN "入院记录"."现住址-村(街、路、弄等)名称" IS '个人现住地址的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "入院记录"."现住址-乡(镇、街道办事处)名称" IS '个人现住地址的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "入院记录"."现地址-乡(镇、街道办事处)代码" IS '个人现住地址的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "入院记录"."现住址-县(市、区)名称" IS '个人现住地址的县(市、区)的名称';
COMMENT ON COLUMN "入院记录"."现住址-县(市、区)代码" IS '个人现住地址的县(市、区)的在特定编码体系中的代码';
COMMENT ON COLUMN "入院记录"."现住址-市(地区、州)名称" IS '个人现住地址的市、地区或州的名称';
COMMENT ON COLUMN "入院记录"."现住址-市(地区、州)代码" IS '个人现住地址的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "入院记录"."现住址-省(自治区、直辖市)名称" IS '个人现住地址的省、自治区或直辖市名称';
COMMENT ON COLUMN "入院记录"."现住址-省(自治区、直辖市)代码" IS '个人现住地址的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "入院记录"."行政区划代码" IS '现住地区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "入院记录"."婚姻状况名称" IS '当前婚姻状况(已婚、未婚、初婚等)在标准特定编码体系中的名称';
COMMENT ON COLUMN "入院记录"."婚姻状况代码" IS '当前婚姻状况(已婚、未婚、初婚等)在标准特定编码体系中的代码';
COMMENT ON COLUMN "入院记录"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "入院记录"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "入院记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "入院记录"."性别代码" IS '患者生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "入院记录"."性别名称" IS '患者生理性别在特定编码体系中的名称';
COMMENT ON COLUMN "入院记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院记录"."证件号码" IS '患者各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "入院记录"."证件类型代码" IS '患者身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "入院记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "入院记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "入院记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "入院记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "入院记录"."病房号" IS '患者入院时，所住病房对应的编号';
COMMENT ON COLUMN "入院记录"."科室名称" IS '患者入院时的科室的机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "入院记录"."科室代码" IS '按照原始的编码规则赋予患者入院时的科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "入院记录"."病区名称" IS '患者入院时，所住病区名称';
COMMENT ON COLUMN "入院记录"."病区代码" IS '患者入院时，所住病区编号';
COMMENT ON COLUMN "入院记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "入院记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "入院记录"."住院次数" IS '患者住院次数统计';
COMMENT ON COLUMN "入院记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "入院记录"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "入院记录"."入院记录编号" IS '患者入院信息数据的本地唯一ID';
COMMENT ON COLUMN "入院记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "入院记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "入院记录"."确定诊断-西医诊断名称" IS '确定诊断西医诊断标准名称';
COMMENT ON COLUMN "入院记录"."发病时间" IS '某种疾病或健康问题在患者身上开始显现或出现第一个症状的时间点';
COMMENT ON COLUMN "入院记录"."症状开始时间" IS '患者的某种症状出现的日期和时间';
COMMENT ON COLUMN "入院记录"."症状停止时间" IS '患者的某种症状停止的日期和时间';
COMMENT ON COLUMN "入院记录"."医疗费用支付方式代码" IS '患者用于支付医疗费用的具体方式或来源在编码体系中的代码';
COMMENT ON COLUMN "入院记录"."医疗费用支付方式名称" IS '患者用于支付医疗费用的具体方式或来源';
COMMENT ON COLUMN "入院记录"."疾病状态代码" IS '住院患者疾病的危急程度在编码体系中的代码';
COMMENT ON COLUMN "入院记录"."疾病状态名称" IS '住院患者疾病的危急程度在编码体系中的名称';
COMMENT ON COLUMN "入院记录"."接诊医师工号" IS '接诊医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "入院记录"."接诊医师姓名" IS '接诊医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院记录"."住院医师工号" IS '所在科室具体负责诊治的，具有住院医师的工号';
COMMENT ON COLUMN "入院记录"."住院医师姓名" IS '所在科室具体负责诊治的，具有住院医师专业技术职务任职资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院记录"."主治医师工号" IS '所在科室的具有主治医师的工号';
COMMENT ON COLUMN "入院记录"."主治医师姓名" IS '所在科室的具有主治医师专业技术职务资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院记录"."主任医师工号" IS '主任医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "入院记录"."主任医师姓名" IS '主任医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "入院记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "入院记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "入院记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "入院记录"."住院症状名称" IS '患者在住院时的症状在编码体系中的名称';
COMMENT ON COLUMN "入院记录"."住院症状代码" IS '患者在住院时的症状在编码体系中的代码';
COMMENT ON COLUMN "入院记录"."入院途径名称" IS '患者入院的方式或来源在编码体系中的名称';
COMMENT ON COLUMN "入院记录"."入院途径代码" IS '患者入院的方式或来源在编码体系中的代码';
COMMENT ON COLUMN "入院记录"."入院原因" IS '患者因某种疾病或健康问题住院治疗的原因的描述';
COMMENT ON COLUMN "入院记录"."入院诊断顺位" IS '表示入院诊断的顺位及其从属关系';
COMMENT ON COLUMN "入院记录"."补充诊断时间" IS '补充诊断下达当日的公元纪年和时间的完整描述';
COMMENT ON COLUMN "入院记录"."补充诊断名称" IS '补充诊断西医诊断标准名称';
COMMENT ON COLUMN "入院记录"."补充诊断代码" IS '按照平台编码规则赋予西医补充诊断疾病的唯一标识';
COMMENT ON COLUMN "入院记录"."确定诊断时间" IS '确定诊断下达当日的公元纪年和时间的完整描述';
COMMENT ON COLUMN "入院记录"."确定诊断-中医证候名称" IS '确定诊断中医证候标准名称';
COMMENT ON COLUMN "入院记录"."确定诊断-中医证候代码" IS '患者入院时按照平台编码规则赋予确定诊断中医证候的唯一标识';
COMMENT ON COLUMN "入院记录"."确定诊断-中医病名名称" IS '患者入院时按照平台编码规则赋予确定诊断中医疾病的名称';
CREATE TABLE IF NOT EXISTS "入院记录" (
"确定诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "确定诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "修正诊断时间" timestamp DEFAULT NULL,
 "修正诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "修正诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "修正诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "修正诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "修正诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "修正诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "初步诊断时间" timestamp DEFAULT NULL,
 "初步诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "初步诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "初步诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "初步诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "初步诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "初步诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "治则治法名称" varchar (100) DEFAULT NULL,
 "治则治法代码" varchar (100) DEFAULT NULL,
 "辨证分型名称" varchar (50) DEFAULT NULL,
 "辨证分型代码" varchar (7) DEFAULT NULL,
 "中医“四诊”观察结果" text,
 "辅助检查结果" varchar (1000) DEFAULT NULL,
 "专科情况" text,
 "神经系统检查结果" text,
 "四肢检查结果" text,
 "脊柱检查结果" text,
 "外生殖器检查结果" text,
 "直肠肛门检查结果描述" text,
 "腹部检查结果" text,
 "胸部检查结果" text,
 "颈部检查结果" text,
 "头部及其器官检查结果" text,
 "全身浅表淋巴结检查结果" text,
 "皮肤和黏膜检查结果" text,
 "一般状况检查结果" text,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "身高(cm)" decimal (4,
 1) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "呼吸频率(次/min)" decimal (3,
 0) DEFAULT NULL,
 "脉率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "家族史" text,
 "月经史" text,
 "婚育史" text,
 "个人史" text,
 "过敏史" text,
 "输血史" text,
 "手术史" text,
 "预防接种史" text,
 "传染病史" text,
 "传染病标志" varchar (1) DEFAULT NULL,
 "疾病史(含外伤)" text,
 "一般健康状况标志" varchar (1) DEFAULT NULL,
 "现病史" text,
 "主诉" text,
 "陈述内容可靠标志" varchar (1) DEFAULT NULL,
 "病史陈述者地址" varchar (100) DEFAULT NULL,
 "病史陈述者联系电话" varchar (20) DEFAULT NULL,
 "陈述者与患者的关系名称" varchar (50) DEFAULT NULL,
 "陈述者与患者的关系代码" varchar (1) DEFAULT NULL,
 "病史陈述者姓名" varchar (50) DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "职业类别名称" varchar (60) DEFAULT NULL,
 "职业类别代码" varchar (4) DEFAULT NULL,
 "现住详细地址" varchar (128) DEFAULT NULL,
 "现住址-门牌号码" varchar (70) DEFAULT NULL,
 "现住址-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "现住址-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "现地址-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "现住址-县(市、区)名称" varchar (70) DEFAULT NULL,
 "现住址-县(市、区)代码" varchar (20) DEFAULT NULL,
 "现住址-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "现住址-市(地区、州)代码" varchar (20) DEFAULT NULL,
 "现住址-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "现住址-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "行政区划代码" varchar (12) DEFAULT NULL,
 "婚姻状况名称" varchar (50) DEFAULT NULL,
 "婚姻状况代码" varchar (2) DEFAULT NULL,
 "民族代码" varchar (2) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病区代码" varchar (20) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "住院就诊流水号" varchar (32) NOT NULL,
 "入院记录编号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "确定诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "发病时间" timestamp DEFAULT NULL,
 "症状开始时间" timestamp DEFAULT NULL,
 "症状停止时间" timestamp DEFAULT NULL,
 "医疗费用支付方式代码" varchar (30) DEFAULT NULL,
 "医疗费用支付方式名称" varchar (100) DEFAULT NULL,
 "疾病状态代码" varchar (1) DEFAULT NULL,
 "疾病状态名称" varchar (200) DEFAULT NULL,
 "接诊医师工号" varchar (20) DEFAULT NULL,
 "接诊医师姓名" varchar (50) DEFAULT NULL,
 "住院医师工号" varchar (20) DEFAULT NULL,
 "住院医师姓名" varchar (50) DEFAULT NULL,
 "主治医师工号" varchar (20) DEFAULT NULL,
 "主治医师姓名" varchar (50) DEFAULT NULL,
 "主任医师工号" varchar (20) DEFAULT NULL,
 "主任医师姓名" varchar (50) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "住院症状名称" varchar (200) DEFAULT NULL,
 "住院症状代码" varchar (100) DEFAULT NULL,
 "入院途径名称" varchar (50) DEFAULT NULL,
 "入院途径代码" varchar (4) DEFAULT NULL,
 "入院原因" varchar (1000) DEFAULT NULL,
 "入院诊断顺位" varchar (20) DEFAULT NULL,
 "补充诊断时间" timestamp DEFAULT NULL,
 "补充诊断名称" varchar (512) DEFAULT NULL,
 "补充诊断代码" varchar (64) DEFAULT NULL,
 "确定诊断时间" timestamp DEFAULT NULL,
 "确定诊断-中医证候名称" varchar (512) DEFAULT NULL,
 "确定诊断-中医证候代码" varchar (64) DEFAULT NULL,
 "确定诊断-中医病名名称" varchar (512) DEFAULT NULL,
 CONSTRAINT "入院记录"_"住院就诊流水号"_"入院记录编号"_"医疗机构代码"_PK PRIMARY KEY ("住院就诊流水号",
 "入院记录编号",
 "医疗机构代码")
);


COMMENT ON TABLE "全员人口个案" IS '人口基本信息表，包括姓名、证件号、电话、地址信息以及配偶、父母相关信息';
COMMENT ON COLUMN "全员人口个案"."医疗机构代码" IS '按照某一特定编码规则赋予医疗机构的唯一标识';
COMMENT ON COLUMN "全员人口个案"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "全员人口个案"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "全员人口个案"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "全员人口个案"."死亡原因" IS '患者死亡原因的详细描述';
COMMENT ON COLUMN "全员人口个案"."死亡日期" IS '患者死亡当日的公元纪年日期';
COMMENT ON COLUMN "全员人口个案"."领独生子女父母光荣证日期" IS '领取独生子女父母光荣证的公元纪年日期';
COMMENT ON COLUMN "全员人口个案"."母亲证件号码" IS '母亲各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "全员人口个案"."母亲证件类型名称" IS '母亲身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "全员人口个案"."母亲证件类型代码" IS '母亲身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."母亲姓名" IS '母亲在公安户籍管理部门正式登记注册的姓氏和名称。未取名者填“C”+生母姓名';
COMMENT ON COLUMN "全员人口个案"."父亲证件号码" IS '父亲各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "全员人口个案"."父亲证件类型名称" IS '父亲身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "全员人口个案"."父亲证件类型代码" IS '父亲身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."父亲姓名" IS '父亲在公安户籍管理部门正式登记注册的姓氏和名称。未取名者填“C”+生母姓名';
COMMENT ON COLUMN "全员人口个案"."配偶户籍地-行政区划代码" IS '配偶户籍地址的6位区县行政区划码+省统计局发布的3位乡镇编码和3位村编码';
COMMENT ON COLUMN "全员人口个案"."配偶户籍地-详细地址" IS '配偶户籍地址的详细描述';
COMMENT ON COLUMN "全员人口个案"."配偶居住地-行政区划代码" IS '配偶现住地址的6位区县行政区划码+省统计局发布的3位乡镇编码和3位村编码';
COMMENT ON COLUMN "全员人口个案"."配偶居住地-详细地址" IS '配偶现住地址的详细描述';
COMMENT ON COLUMN "全员人口个案"."配偶户口性质名称" IS '配偶户口性质（如非农业户口、农业户口）在特定编码体系中的名称';
COMMENT ON COLUMN "全员人口个案"."配偶户口性质代码" IS '配偶户口性质（如非农业户口、农业户口）在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."配偶学历名称" IS '配偶受教育最高程度的类别标准名称，如研究生教育、大学本科、专科教育等';
COMMENT ON COLUMN "全员人口个案"."配偶学历代码" IS '配偶受教育最高程度的类别(如研究生教育、大学本科、专科教育等)在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."配偶民族名称" IS '配偶所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "全员人口个案"."配偶民族代码" IS '配偶所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."配偶出生日期" IS '配偶出生当日的公元纪年日期';
COMMENT ON COLUMN "全员人口个案"."配偶证件号码" IS '配偶各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "全员人口个案"."配偶证件类型名称" IS '配偶身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "全员人口个案"."配偶证件类型代码" IS '配偶身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."配偶姓名" IS '配偶在公安户籍管理部门正式登记注册的姓氏和名称。未取名者填“C”+生母姓名';
COMMENT ON COLUMN "全员人口个案"."职业类别名称" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的名称';
COMMENT ON COLUMN "全员人口个案"."职业类别代码" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."工作单位" IS '个体工作单位的组织机构名称';
COMMENT ON COLUMN "全员人口个案"."联系电话号码" IS '本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "全员人口个案"."与户主关系名称" IS '家庭成员与户主关系的标准名称,如配偶、子女、父母等';
COMMENT ON COLUMN "全员人口个案"."与户主关系代码" IS '家庭成员与户主关系的家庭关系在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."家庭户编号" IS '按照特定编码规则赋予家庭户的唯一标识';
COMMENT ON COLUMN "全员人口个案"."进入现居住地时间" IS '进入现居住地时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "全员人口个案"."离开户籍地时间" IS '离开户籍地时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "全员人口个案"."初婚日期" IS '第一次结婚时的公元纪年日期';
COMMENT ON COLUMN "全员人口个案"."婚姻状况名称" IS '当前婚姻状况的标准名称，如已婚、未婚、初婚等';
COMMENT ON COLUMN "全员人口个案"."婚姻状况代码" IS '当前婚姻状况(已婚、未婚、初婚等)在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."户口性质名称" IS '个体户口性质（如非农业户口、农业户口）在特定编码体系中的名称';
COMMENT ON COLUMN "全员人口个案"."户口性质代码" IS '个体户口性质（如非农业户口、农业户口）在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."学历名称" IS '个体受教育最高程度的类别标准名称，如研究生教育、大学本科、专科教育等';
COMMENT ON COLUMN "全员人口个案"."学历代码" IS '个体受教育最高程度的类别(如研究生教育、大学本科、专科教育等)在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."民族名称" IS '所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "全员人口个案"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."国籍名称" IS '所属国籍在特定编码体系中的名称';
COMMENT ON COLUMN "全员人口个案"."国籍代码" IS '所属国籍在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."出生地-行政区划代码" IS '出生地址的6位区县行政区划码+省统计局发布的3位乡镇编码和3位村编码';
COMMENT ON COLUMN "全员人口个案"."出生地-详细地址" IS '出生地址的详细描述';
COMMENT ON COLUMN "全员人口个案"."户籍地-行政区划代码" IS '户籍地址的6位区县行政区划码+省统计局发布的3位乡镇编码和3位村编码';
COMMENT ON COLUMN "全员人口个案"."户籍地-详细地址" IS '户籍地址的详细描述';
COMMENT ON COLUMN "全员人口个案"."居住地-行政区划代码" IS '居住地址的6位区县行政区划码+省统计局发布的3位乡镇编码和3位村编码';
COMMENT ON COLUMN "全员人口个案"."居住地-详细地址" IS '现住地址的详细描述';
COMMENT ON COLUMN "全员人口个案"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "全员人口个案"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "全员人口个案"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "全员人口个案"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "全员人口个案"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "全员人口个案"."姓名" IS '本人在公安户籍管理部门正式登记注册的姓氏和名称。未取名者填“C”+生母姓名';
COMMENT ON COLUMN "全员人口个案"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "全员人口个案"."全员人口个案标识号" IS '按照某一特定编码规则赋予全员人口个案的唯一标识号';
COMMENT ON COLUMN "全员人口个案"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
CREATE TABLE IF NOT EXISTS "全员人口个案" (
"医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "死亡原因" text,
 "死亡日期" date DEFAULT NULL,
 "领独生子女父母光荣证日期" date DEFAULT NULL,
 "母亲证件号码" varchar (18) DEFAULT NULL,
 "母亲证件类型名称" varchar (50) DEFAULT NULL,
 "母亲证件类型代码" varchar (2) DEFAULT NULL,
 "母亲姓名" varchar (50) DEFAULT NULL,
 "父亲证件号码" varchar (32) DEFAULT NULL,
 "父亲证件类型名称" varchar (50) DEFAULT NULL,
 "父亲证件类型代码" varchar (2) DEFAULT NULL,
 "父亲姓名" varchar (50) DEFAULT NULL,
 "配偶户籍地-行政区划代码" varchar (12) DEFAULT NULL,
 "配偶户籍地-详细地址" varchar (200) DEFAULT NULL,
 "配偶居住地-行政区划代码" varchar (12) DEFAULT NULL,
 "配偶居住地-详细地址" varchar (200) DEFAULT NULL,
 "配偶户口性质名称" varchar (50) DEFAULT NULL,
 "配偶户口性质代码" varchar (2) DEFAULT NULL,
 "配偶学历名称" varchar (50) DEFAULT NULL,
 "配偶学历代码" varchar (5) DEFAULT NULL,
 "配偶民族名称" varchar (50) DEFAULT NULL,
 "配偶民族代码" varchar (2) DEFAULT NULL,
 "配偶出生日期" date DEFAULT NULL,
 "配偶证件号码" varchar (32) DEFAULT NULL,
 "配偶证件类型名称" varchar (50) DEFAULT NULL,
 "配偶证件类型代码" varchar (2) DEFAULT NULL,
 "配偶姓名" varchar (50) DEFAULT NULL,
 "职业类别名称" varchar (60) DEFAULT NULL,
 "职业类别代码" varchar (4) DEFAULT NULL,
 "工作单位" varchar (128) DEFAULT NULL,
 "联系电话号码" varchar (20) DEFAULT NULL,
 "与户主关系名称" varchar (50) DEFAULT NULL,
 "与户主关系代码" varchar (2) DEFAULT NULL,
 "家庭户编号" varchar (64) DEFAULT NULL,
 "进入现居住地时间" timestamp DEFAULT NULL,
 "离开户籍地时间" timestamp DEFAULT NULL,
 "初婚日期" date DEFAULT NULL,
 "婚姻状况名称" varchar (50) DEFAULT NULL,
 "婚姻状况代码" varchar (2) DEFAULT NULL,
 "户口性质名称" varchar (50) DEFAULT NULL,
 "户口性质代码" varchar (2) DEFAULT NULL,
 "学历名称" varchar (50) DEFAULT NULL,
 "学历代码" varchar (5) DEFAULT NULL,
 "民族名称" varchar (50) DEFAULT NULL,
 "民族代码" varchar (2) DEFAULT NULL,
 "国籍名称" varchar (50) DEFAULT NULL,
 "国籍代码" varchar (10) DEFAULT NULL,
 "出生地-行政区划代码" varchar (12) DEFAULT NULL,
 "出生地-详细地址" varchar (200) DEFAULT NULL,
 "户籍地-行政区划代码" varchar (12) DEFAULT NULL,
 "户籍地-详细地址" varchar (200) DEFAULT NULL,
 "居住地-行政区划代码" varchar (12) DEFAULT NULL,
 "居住地-详细地址" varchar (200) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "全员人口个案标识号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 CONSTRAINT "全员人口个案"_"医疗机构代码"_"全员人口个案标识号"_PK PRIMARY KEY ("医疗机构代码",
 "全员人口个案标识号")
);


COMMENT ON TABLE "出入量记录" IS '患者出量、入量数据，包括用药明细数据';
COMMENT ON COLUMN "出入量记录"."呕吐标志" IS '标识对患者是有呕吐症状的标志';
COMMENT ON COLUMN "出入量记录"."护理操作项目类目名称" IS '多个护理操作项目的名称';
COMMENT ON COLUMN "出入量记录"."护理操作名称" IS '进行护理操作的具体名称';
COMMENT ON COLUMN "出入量记录"."护理观察项目名称" IS '护理观察项目的名称，如患者神志状态、饮食情况，皮肤情况、氧疗情况、排尿排便情况，流量、出量、人量等等，根据护理内容的不同选择不同的观察项目名称';
COMMENT ON COLUMN "出入量记录"."护理类型代码" IS '护理类型的分类在特定编码体系中的代码';
COMMENT ON COLUMN "出入量记录"."护理等级代码" IS '护理级别的分类在特定编码体系中的代码';
COMMENT ON COLUMN "出入量记录"."疾病诊断名称" IS '疾病诊断在特定编码体系中的名称';
COMMENT ON COLUMN "出入量记录"."疾病诊断代码" IS '疾病诊断在特定编码体系中的代码';
COMMENT ON COLUMN "出入量记录"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "出入量记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "出入量记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "出入量记录"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "出入量记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "出入量记录"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "出入量记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "出入量记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "出入量记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "出入量记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "出入量记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "出入量记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "出入量记录"."就诊次数" IS '对于门(急)诊患者，此处表示患者门(急)诊的累计次数；对于住院患者，此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "出入量记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号';
COMMENT ON COLUMN "出入量记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "出入量记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "出入量记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "出入量记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "出入量记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "出入量记录"."出入量记录流水号" IS '按照某一特定编码规则赋予出入量记录的顺序号';
COMMENT ON COLUMN "出入量记录"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "出入量记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "出入量记录"."护理操作结果" IS '护理操作结果的详细描述';
COMMENT ON COLUMN "出入量记录"."护理观察结果" IS '对护理观察项目结果的详细描述';
COMMENT ON COLUMN "出入量记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "出入量记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "出入量记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "出入量记录"."记录时间" IS '完成记录时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "出入量记录"."签名时间" IS '护理护士在护理记录上完成签名的公元纪年和日期的完整描述';
COMMENT ON COLUMN "出入量记录"."护士工号" IS '护理护士的工号';
COMMENT ON COLUMN "出入量记录"."护士姓名" IS '护理护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "出入量记录"."饮食描述" IS '描述患者的饮食内容，例如母乳、牛奶、米饭等';
COMMENT ON COLUMN "出入量记录"."用药途径代码" IS '药物使用途径(如口服、静滴、喷喉等)在特定编码体系中的代码';
COMMENT ON COLUMN "出入量记录"."药物使用总剂量" IS '在一定时间段内使用药物的总量。根据单次剂量、频次计算得到的总量';
COMMENT ON COLUMN "出入量记录"."使用剂量单位" IS '使用次剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "出入量记录"."每次使用剂量" IS '单次使用的剂量，按剂量单位计';
COMMENT ON COLUMN "出入量记录"."药物剂型代码" IS '药物剂型类别(如颗粒剂、片剂、丸剂、胶囊剂等)在特定编码体系中的代码';
COMMENT ON COLUMN "出入量记录"."用药频次名称" IS '单位时间内药物使用频次类别的标准名称，如每天两次、每周两次、睡前一次等';
COMMENT ON COLUMN "出入量记录"."用药频次代码" IS '单位时间内药物使用的频次类别(如每天两次、每周两次、睡前一次等)在特定编码体系中的代码';
COMMENT ON COLUMN "出入量记录"."药品用法" IS '对治疗疾病所用药物的具体服用方法的详细描述';
COMMENT ON COLUMN "出入量记录"."药物名称" IS '药品在特定编码体系中的名称。对于医疗机构制剂，此处填写医疗机构制剂名称';
COMMENT ON COLUMN "出入量记录"."出入量用药明细流水号" IS '按照某一特性编码规则赋予本次出入量唯一标志的顺序号。针对门(急)诊患者，此处指处方流水号；针对住院患者，此处指医嘱ID';
COMMENT ON COLUMN "出入量记录"."其他" IS '出入量记录中出量-其他出量记录值，单位为mL/日';
COMMENT ON COLUMN "出入量记录"."胃液量" IS '出入量记录中出量-胃液量记录值，单位为mL/日';
COMMENT ON COLUMN "出入量记录"."小便失禁标志" IS '标识对患者是有小便失禁症状的标志';
COMMENT ON COLUMN "出入量记录"."大便失禁标志" IS '标识对患者是有大便失禁症状的标志';
COMMENT ON COLUMN "出入量记录"."排便困难标志" IS '标识对患者是有排便困难症状的标志';
COMMENT ON COLUMN "出入量记录"."大便次数(次/日)" IS '每天大便的次数，计数单位次/d';
COMMENT ON COLUMN "出入量记录"."尿量(mL/日)" IS '出入量记录中出量-尿量记录值，单位为mL/日';
COMMENT ON COLUMN "出入量记录"."排尿困难标志" IS '标识对患者是有排尿困难症状的标志';
CREATE TABLE IF NOT EXISTS "出入量记录" (
"呕吐标志" varchar (1) DEFAULT NULL,
 "护理操作项目类目名称" varchar (100) DEFAULT NULL,
 "护理操作名称" varchar (100) DEFAULT NULL,
 "护理观察项目名称" varchar (200) DEFAULT NULL,
 "护理类型代码" varchar (1) DEFAULT NULL,
 "护理等级代码" varchar (1) DEFAULT NULL,
 "疾病诊断名称" varchar (512) DEFAULT NULL,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "出入量记录流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "护理操作结果" text,
 "护理观察结果" text,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "记录时间" timestamp DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "护士工号" varchar (20) DEFAULT NULL,
 "护士姓名" varchar (50) DEFAULT NULL,
 "饮食描述" varchar (20) DEFAULT NULL,
 "用药途径代码" varchar (4) DEFAULT NULL,
 "药物使用总剂量" decimal (11,
 2) DEFAULT NULL,
 "使用剂量单位" varchar (64) DEFAULT NULL,
 "每次使用剂量" decimal (15,
 3) DEFAULT NULL,
 "药物剂型代码" varchar (4) DEFAULT NULL,
 "用药频次名称" varchar (32) DEFAULT NULL,
 "用药频次代码" varchar (32) DEFAULT NULL,
 "药品用法" varchar (32) DEFAULT NULL,
 "药物名称" varchar (50) DEFAULT NULL,
 "出入量用药明细流水号" varchar (64) DEFAULT NULL,
 "其他" varchar (100) DEFAULT NULL,
 "胃液量" decimal (20,
 0) DEFAULT NULL,
 "小便失禁标志" varchar (1) DEFAULT NULL,
 "大便失禁标志" varchar (1) DEFAULT NULL,
 "排便困难标志" varchar (1) DEFAULT NULL,
 "大便次数(次/日)" decimal (3,
 0) DEFAULT NULL,
 "尿量(mL/日)" varchar (20) DEFAULT NULL,
 "排尿困难标志" varchar (1) DEFAULT NULL,
 CONSTRAINT "出入量记录"_"出入量记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("出入量记录流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "出院评估记录" IS '出院时的各项评估指标结果';
COMMENT ON COLUMN "出院评估记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "出院评估记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "出院评估记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "出院评估记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "出院评估记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "出院评估记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "出院评估记录"."签名时间" IS '护理护士在护理记录上完成签名的公元纪年和日期的完整描述';
COMMENT ON COLUMN "出院评估记录"."出院评估记录流水号" IS '按照某一特定编码规则赋予出院评估记录的顺序号';
COMMENT ON COLUMN "出院评估记录"."护士姓名" IS '护理护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "出院评估记录"."护士工号" IS '护理护士的工号';
COMMENT ON COLUMN "出院评估记录"."复诊指导" IS '患者出院后再次就诊情况指导';
COMMENT ON COLUMN "出院评估记录"."宣教内容" IS '医护人员对服务对象进行相关宣传指导活动的详细描述';
COMMENT ON COLUMN "出院评估记录"."生活方式指导" IS '对患者出院后进行生活方式指导';
COMMENT ON COLUMN "出院评估记录"."饮食指导名称" IS '饮食指导类别(如普通饮食、软食、半流食、流食、禁食等)名称';
COMMENT ON COLUMN "出院评估记录"."饮食指导代码" IS '饮食指导类别(如普通饮食、软食、半流食、流食、禁食等)在特定编码体系中的代码';
COMMENT ON COLUMN "出院评估记录"."用药指导" IS '对患者出院后用药方法指导的详细描述';
COMMENT ON COLUMN "出院评估记录"."离院方式名称" IS '患者本次住院离开医院的方式(如正常出院、转院、要求出院、私自离院、死亡等)';
COMMENT ON COLUMN "出院评估记录"."离院方式代码" IS '患者本次住院离开医院的方式(如正常出院、转院、要求出院、私自离院、死亡等)在特定编码体系中的代码';
COMMENT ON COLUMN "出院评估记录"."出院情况" IS '对患者出院情况的详细描述';
COMMENT ON COLUMN "出院评估记录"."生活自理能力名称" IS '自己基本生活照料能力在特定编码体系中的名称';
COMMENT ON COLUMN "出院评估记录"."自理能力代码" IS '自己基本生活照料能力在特定编码体系中的代码';
COMMENT ON COLUMN "出院评估记录"."饮食情况名称" IS '个体饮食情况所属类别的标准名称，如良好、一般、较差等';
COMMENT ON COLUMN "出院评估记录"."饮食情况代码" IS '个体饮食情况所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "出院评估记录"."出院时间" IS '患者实际办理出院手续时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "出院评估记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "出院评估记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "出院评估记录"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "出院评估记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "出院评估记录"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "出院评估记录"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "出院评估记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "出院评估记录"."出院诊断名称" IS '出院诊断断在特定编码体系中的名称';
COMMENT ON COLUMN "出院评估记录"."出院诊断代码" IS '出院诊断在特定编码体系中的代码';
COMMENT ON COLUMN "出院评估记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "出院评估记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "出院评估记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "出院评估记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "出院评估记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "出院评估记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "出院评估记录"."住院次数" IS '此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "出院评估记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "出院评估记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "出院评估记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
CREATE TABLE IF NOT EXISTS "出院评估记录" (
"数据更新时间" timestamp DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "出院评估记录流水号" varchar (64) NOT NULL,
 "护士姓名" varchar (50) DEFAULT NULL,
 "护士工号" varchar (20) DEFAULT NULL,
 "复诊指导" varchar (100) DEFAULT NULL,
 "宣教内容" varchar (100) DEFAULT NULL,
 "生活方式指导" varchar (50) DEFAULT NULL,
 "饮食指导名称" varchar (100) DEFAULT NULL,
 "饮食指导代码" varchar (2) DEFAULT NULL,
 "用药指导" varchar (100) DEFAULT NULL,
 "离院方式名称" varchar (100) DEFAULT NULL,
 "离院方式代码" varchar (1) DEFAULT NULL,
 "出院情况" text,
 "生活自理能力名称" varchar (50) DEFAULT NULL,
 "自理能力代码" varchar (1) DEFAULT NULL,
 "饮食情况名称" varchar (50) DEFAULT NULL,
 "饮食情况代码" varchar (2) DEFAULT NULL,
 "出院时间" timestamp DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "出院诊断名称" varchar (512) DEFAULT NULL,
 "出院诊断代码" varchar (64) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 CONSTRAINT "出院评估记录"_"医疗机构代码"_"出院评估记录流水号"_PK PRIMARY KEY ("医疗机构代码",
 "出院评估记录流水号")
);


COMMENT ON TABLE "医学影像检查报告数量日汇总" IS '实验室日报告单数量，包括门诊报告单数、住院报告单数、生化报告单数等';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."内窥镜类报告单份数" IS '日期内内窥镜类报告的总份数';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."心电类报告单份数" IS '日期内心电类报告的总份数';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."其他类报告单份数" IS '日期内无法归入上述单项类别检查报告的总份数';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."检查人次" IS '日期内所有的检查人次';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."门诊检查人次" IS '日期内门诊类业务的检查人次';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."住院检查人次" IS '日期内住院类业务的检查人次';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."其他业务检查人次" IS '日期内非门诊非住院类业务，例如体检类业务或外单位委托检查的检查人次';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."业务日期" IS '业务交易发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."报告单总份数" IS '日期内所有检查报告的总份数';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."门诊报告单份数" IS '日期内门诊类业务检查报告总份数';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."住院报告单份数" IS '日期内住院类业务检查报告总份数';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."其他业务报告单份数" IS '日期内非门诊非住院类业务，例如体检类业务或外单位委托检查的报告总份数';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."放射类报告单份数" IS '日期内X光、CT等报告的总份数';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."超声类报告单份数" IS '日期内超声类报告的总份数';
COMMENT ON COLUMN "医学影像检查报告数量日汇总"."病理类报告单份数" IS '日期内病理类报告的总份数';
CREATE TABLE IF NOT EXISTS "医学影像检查报告数量日汇总" (
"内窥镜类报告单份数" decimal (9,
 0) DEFAULT NULL,
 "心电类报告单份数" decimal (9,
 0) DEFAULT NULL,
 "其他类报告单份数" decimal (9,
 0) DEFAULT NULL,
 "检查人次" decimal (9,
 0) DEFAULT NULL,
 "门诊检查人次" decimal (9,
 0) DEFAULT NULL,
 "住院检查人次" decimal (9,
 0) DEFAULT NULL,
 "其他业务检查人次" decimal (9,
 0) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "业务日期" date NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "报告单总份数" decimal (9,
 0) DEFAULT NULL,
 "门诊报告单份数" decimal (9,
 0) DEFAULT NULL,
 "住院报告单份数" decimal (9,
 0) DEFAULT NULL,
 "其他业务报告单份数" decimal (9,
 0) DEFAULT NULL,
 "放射类报告单份数" decimal (9,
 0) DEFAULT NULL,
 "超声类报告单份数" decimal (9,
 0) DEFAULT NULL,
 "病理类报告单份数" decimal (9,
 0) DEFAULT NULL,
 CONSTRAINT "医学影像检查报告数量日汇总"_"医疗机构代码"_"业务日期"_PK PRIMARY KEY ("医疗机构代码",
 "业务日期")
);


COMMENT ON TABLE "医生坐诊日志登记表" IS '医生每日坐诊日志，包括坐诊日期、科室、坐诊类型及坐诊时间段等';
COMMENT ON COLUMN "医生坐诊日志登记表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "医生坐诊日志登记表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "医生坐诊日志登记表"."科室代码" IS '按照特定编码规则赋予医生所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "医生坐诊日志登记表"."科室名称" IS '医生所在科室在特定编码体系中的名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "医生坐诊日志登记表"."入院诊断医生工号" IS '入院诊断医生在特定编码体系中的顺序号';
COMMENT ON COLUMN "医生坐诊日志登记表"."坐诊医生姓名" IS '坐诊医生在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医生坐诊日志登记表"."坐诊日期" IS '医生坐诊当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "医生坐诊日志登记表"."坐诊类型" IS '医生坐诊类型的描述';
COMMENT ON COLUMN "医生坐诊日志登记表"."时间段" IS '医生坐诊时间段描述，如8:00-10:00';
COMMENT ON COLUMN "医生坐诊日志登记表"."签到标志" IS '标识医生是否签到';
COMMENT ON COLUMN "医生坐诊日志登记表"."签到时间" IS '医生签到当日的公元纪年日期和时间的详细描述';
COMMENT ON COLUMN "医生坐诊日志登记表"."坐诊地址" IS '医生坐诊地址的详细描述';
COMMENT ON COLUMN "医生坐诊日志登记表"."备注" IS '医生其他坐诊信息的补充说明';
COMMENT ON COLUMN "医生坐诊日志登记表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "医生坐诊日志登记表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医生坐诊日志登记表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医生坐诊日志登记表"."分诊台编号" IS '按照一定编码规则赋予分诊台的顺序号';
COMMENT ON COLUMN "医生坐诊日志登记表"."医疗机构代码" IS '按照某一特定编码规则赋予医疗机构的唯一标识';
CREATE TABLE IF NOT EXISTS "医生坐诊日志登记表" (
"医疗机构名称" varchar (70) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "入院诊断医生工号" varchar (20) DEFAULT NULL,
 "坐诊医生姓名" varchar (50) DEFAULT NULL,
 "坐诊日期" date DEFAULT NULL,
 "坐诊类型" varchar (30) DEFAULT NULL,
 "时间段" varchar (8) DEFAULT NULL,
 "签到标志" varchar (1) DEFAULT NULL,
 "签到时间" timestamp DEFAULT NULL,
 "坐诊地址" varchar (100) DEFAULT NULL,
 "备注" varchar (1000) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "分诊台编号" varchar (8) NOT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 CONSTRAINT "医生坐诊日志登记表"_"分诊台编号"_"医疗机构代码"_PK PRIMARY KEY ("分诊台编号",
 "医疗机构代码")
);


COMMENT ON TABLE "医疗保险日结数据_机构(周期)" IS '医疗保险周期日结数据，如实际住院补偿总费用、城乡居民大病保险实际补偿总费用等';
COMMENT ON COLUMN "医疗保险日结数据_机构(周期)"."机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "医疗保险日结数据_机构(周期)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医疗保险日结数据_机构(周期)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医疗保险日结数据_机构(周期)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "医疗保险日结数据_机构(周期)"."城乡居民大病保险实际补偿的总人次数" IS '统计周期内的基层医疗卫生机构城乡居民大病保险实际住院补偿的总人次';
COMMENT ON COLUMN "医疗保险日结数据_机构(周期)"."城乡居民大病保险实际补偿总费用" IS '统计周期内的基层医疗卫生机构城乡居民大病保险实际住院补偿部分的总费用';
COMMENT ON COLUMN "医疗保险日结数据_机构(周期)"."住院总费用" IS '统计周期内的基层医疗卫生机构住院收入总费用';
COMMENT ON COLUMN "医疗保险日结数据_机构(周期)"."实际住院补偿总费用" IS '统计周期内的基层医疗卫生机构实际住院补偿部分的总费用';
COMMENT ON COLUMN "医疗保险日结数据_机构(周期)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "医疗保险日结数据_机构(周期)"."统计日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "医疗保险日结数据_机构(周期)"."机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
CREATE TABLE IF NOT EXISTS "医疗保险日结数据_机构(
周期)" ("机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "城乡居民大病保险实际补偿的总人次数" decimal (10,
 0) DEFAULT NULL,
 "城乡居民大病保险实际补偿总费用" decimal (10,
 2) DEFAULT NULL,
 "住院总费用" decimal (10,
 2) DEFAULT NULL,
 "实际住院补偿总费用" decimal (10,
 2) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "统计日期" date NOT NULL,
 "机构名称" varchar (200) DEFAULT NULL,
 CONSTRAINT "医疗保险日结数据_机构(周期)"_"机构代码"_"统计日期"_PK PRIMARY KEY ("机构代码",
 "统计日期")
);


COMMENT ON TABLE "医疗设备表" IS '医疗设备使用管理记录，包括设备类型、型号、产地、购买日期、使用情况、年检信息等';
COMMENT ON COLUMN "医疗设备表"."购进时新旧情况代码" IS '设备购进时情况(如启用、未启用、报废等)在特定编码体系中的代码';
COMMENT ON COLUMN "医疗设备表"."购买日期" IS '购买当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "医疗设备表"."生产厂家名称" IS '生产该类设备的公司在工商局注册、审批通过后的厂家名称';
COMMENT ON COLUMN "医疗设备表"."产地类别代码" IS '设备产地(如进口、国产、合资等)在特定编码体系中的代码';
COMMENT ON COLUMN "医疗设备表"."同批购进相同型号设备台数" IS '相同批次购进的相同型号的设备数量';
COMMENT ON COLUMN "医疗设备表"."大型设备标志" IS '本设备是否为大型设备的标志';
COMMENT ON COLUMN "医疗设备表"."设备型号" IS '检查设备的出厂型号，如梅里埃vitek32、BD';
COMMENT ON COLUMN "医疗设备表"."设备类型名称" IS '设备类型(如X射线诊断设备、超声诊断设备、放射治疗设备等)在特定编码体系中的名称';
COMMENT ON COLUMN "医疗设备表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医疗设备表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医疗设备表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "医疗设备表"."年检结论" IS '对设备检查的结论的详细描述';
COMMENT ON COLUMN "医疗设备表"."年检审核员" IS '年检审核员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医疗设备表"."年检检验员" IS '年检检验员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医疗设备表"."年检单位名称" IS '负责年检的企业在工商局注册、审批通过后的厂家名称';
COMMENT ON COLUMN "医疗设备表"."年检时间" IS '进行年检当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医疗设备表"."本年度年检标志" IS '设备本年度是否进行了年检的标志';
COMMENT ON COLUMN "医疗设备表"."填报时间" IS '数据填报当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医疗设备表"."资产原值" IS '本设备的固定资产原值估算，计量单位为人民币元，默认为0';
COMMENT ON COLUMN "医疗设备表"."资产项目分类" IS '设备项目分类(如甲类、乙类等)在特定编码体系中的代码';
COMMENT ON COLUMN "医疗设备表"."急救车配备车载卫星标志" IS '急救车是否已配备车载卫星标志';
COMMENT ON COLUMN "医疗设备表"."使用情况" IS '设备使用情况(如启用、未启用、报废等)在特定编码体系中的名称';
COMMENT ON COLUMN "医疗设备表"."理论设计寿命(年)" IS '医疗设备的理论使用寿命，计量单位为年';
COMMENT ON COLUMN "医疗设备表"."购买单价(千元,人民币)" IS '医疗设备购买时的单价，计量单位为元';
COMMENT ON COLUMN "医疗设备表"."购进时新旧情况" IS '设备购进时情况(如启用、未启用、报废等)在特定编码体系中的名称';
COMMENT ON COLUMN "医疗设备表"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "医疗设备表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "医疗设备表"."医疗设备记录序号" IS '按照某一特定编码规则赋予医疗设备记录的顺序号，是医疗设备记录的唯一标识';
COMMENT ON COLUMN "医疗设备表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "医疗设备表"."填报状态代码" IS '设备填报状态(如未到、在库、报废、待修等)在特定编码体系中的代码';
COMMENT ON COLUMN "医疗设备表"."设备类型代号" IS '设备类型(如X射线诊断设备、超声诊断设备、放射治疗设备等)在特定编码体系中的代码';
COMMENT ON COLUMN "医疗设备表"."使用情况代码" IS '设备使用情况(如启用、未启用、报废等)在特定编码体系中的代码';
CREATE TABLE IF NOT EXISTS "医疗设备表" (
"购进时新旧情况代码" varchar (2) DEFAULT NULL,
 "购买日期" date DEFAULT NULL,
 "生产厂家名称" varchar (70) DEFAULT NULL,
 "产地类别代码" varchar (2) DEFAULT NULL,
 "同批购进相同型号设备台数" decimal (5,
 0) DEFAULT NULL,
 "大型设备标志" varchar (1) DEFAULT NULL,
 "设备型号" varchar (100) DEFAULT NULL,
 "设备类型名称" varchar (50) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "年检结论" varchar (200) DEFAULT NULL,
 "年检审核员" varchar (200) DEFAULT NULL,
 "年检检验员" varchar (200) DEFAULT NULL,
 "年检单位名称" varchar (200) DEFAULT NULL,
 "年检时间" timestamp DEFAULT NULL,
 "本年度年检标志" varchar (1) DEFAULT NULL,
 "填报时间" timestamp DEFAULT NULL,
 "资产原值" decimal (20,
 2) DEFAULT NULL,
 "资产项目分类" varchar (4) DEFAULT NULL,
 "急救车配备车载卫星标志" varchar (1) DEFAULT NULL,
 "使用情况" varchar (50) DEFAULT NULL,
 "理论设计寿命(年)" varchar (5) DEFAULT NULL,
 "购买单价(千元,
人民币)" decimal (20,
 2) DEFAULT NULL,
 "购进时新旧情况" varchar (50) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗设备记录序号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "填报状态代码" varchar (2) DEFAULT NULL,
 "设备类型代号" varchar (10) DEFAULT NULL,
 "使用情况代码" varchar (2) DEFAULT NULL,
 CONSTRAINT "医疗设备表"_"医疗机构代码"_"医疗设备记录序号"_PK PRIMARY KEY ("医疗机构代码",
 "医疗设备记录序号")
);


COMMENT ON TABLE "医院感染手术记录" IS '医院感染手术的详细记录，包括手术名称、部位、体位、过程描述等';
COMMENT ON COLUMN "医院感染手术记录"."手术级别代码" IS '按照手术分级管理制度，根据风险性和难易程度不同划分的手术级别(如一级手术、二级手术、三级手术、四级手术)在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染手术记录"."手术结束时间" IS '对患者结束手术操作时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院感染手术记录"."手术开始时间" IS '以手术操作刀碰皮(切皮)时点计算，经自然腔道到达手术部位，没有皮肤切口的手术，开始时间以手术器械进入人体腔道的时点为准,对患者开始手术操作时的公元纪年日期和时间的完整描述。';
COMMENT ON COLUMN "医院感染手术记录"."手术间编号" IS '对患者实施手术操作时所在的手术室房间工号';
COMMENT ON COLUMN "医院感染手术记录"."术前诊断名称" IS '术前诊断在机构内编码规则中对应的名称，如有多个用,隔开';
COMMENT ON COLUMN "医院感染手术记录"."术前诊断代码" IS '按照机构内编码规则赋予术前诊断疾病的唯一标识，如有多个,用隔开';
COMMENT ON COLUMN "医院感染手术记录"."手术序列号" IS '按照某一特定编码规则赋予手术的唯一标识';
COMMENT ON COLUMN "医院感染手术记录"."病床号" IS '按照某一特定编码规则赋予患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "医院感染手术记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "医院感染手术记录"."病区名称" IS '患者当前所在病区在特定编码体系中的名称';
COMMENT ON COLUMN "医院感染手术记录"."住院次数" IS '表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "医院感染手术记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "医院感染手术记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "医院感染手术记录"."Rh血型名称" IS '为患者实际输入的Rh血型的类别在特定编码体系中的名称';
COMMENT ON COLUMN "医院感染手术记录"."Rh血型代码" IS '为患者实际输入的Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染手术记录"."ABO血型名称" IS '为患者实际输入的ABO血型类别在特定编码体系中的名称';
COMMENT ON COLUMN "医院感染手术记录"."ABO血型代码" IS '为患者实际输入的ABO血型类别在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染手术记录"."年龄(月)" IS '条件必填，年龄不足1?周岁的实足年龄的月龄，以分数形式表示：分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1?个月的天数。此时患者年龄(岁)填写值为“0”';
COMMENT ON COLUMN "医院感染手术记录"."年龄(岁)" IS '患者年龄满1 周岁的实足年龄，为患者出生后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "医院感染手术记录"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "医院感染手术记录"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "医院感染手术记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染手术记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医院感染手术记录"."感染记录编号" IS '按照特定编码规则赋予感染记录唯一标志的顺序号';
COMMENT ON COLUMN "医院感染手术记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "医院感染手术记录"."就诊科室名称" IS '就诊科室的机构内名称';
COMMENT ON COLUMN "医院感染手术记录"."就诊科室代码" IS '按照机构内编码规则赋予就诊科室的唯一标识';
COMMENT ON COLUMN "医院感染手术记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "医院感染手术记录"."医院感染手术记录编号" IS '按照特定编码规则赋予医院感染手术记录唯一标志的顺序号';
COMMENT ON COLUMN "医院感染手术记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "医院感染手术记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "医院感染手术记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院感染手术记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院感染手术记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "医院感染手术记录"."文本内容" IS '文本详细内容';
COMMENT ON COLUMN "医院感染手术记录"."择取消手术标志" IS '标识是否择期手术取消的标志';
COMMENT ON COLUMN "医院感染手术记录"."择期手术标志" IS '标识是否择期手术的标志';
COMMENT ON COLUMN "医院感染手术记录"."预防使用抗菌药物天数" IS '预防使用抗孕药物的实际天数';
COMMENT ON COLUMN "医院感染手术记录"."预防使用抗菌药物标志" IS '标识是否预防使用抗菌药物的标志';
COMMENT ON COLUMN "医院感染手术记录"."择期标志" IS '标识是否择期的标志';
COMMENT ON COLUMN "医院感染手术记录"."重返手术标志" IS '标识是否重返手术的标志';
COMMENT ON COLUMN "医院感染手术记录"."术后诊断名称" IS '术后诊断在机构内编码规则中对应的名称，如有多个用,隔开';
COMMENT ON COLUMN "医院感染手术记录"."术后诊断代码" IS '按照机构内编码规则赋予术后诊断疾病的唯一标识，如有多个,用隔开';
COMMENT ON COLUMN "医院感染手术记录"."手术切口愈合等级名称" IS '手术切口愈合类别(如甲、乙、丙)的标准名称';
COMMENT ON COLUMN "医院感染手术记录"."手术切口愈合等级代码" IS '手术切口愈合类别(如甲、乙、丙)在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染手术记录"."出复苏室时间" IS '患者离开复苏室的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院感染手术记录"."入复苏室时间" IS '患者进入复苏室的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院感染手术记录"."麻醉合并症描述" IS '麻醉合并症的详细描述';
COMMENT ON COLUMN "医院感染手术记录"."麻醉合并症标志" IS '标识是否具有麻醉合并症的标志';
COMMENT ON COLUMN "医院感染手术记录"."麻醉结束时间" IS '麻醉结束时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院感染手术记录"."麻醉开始时间" IS '麻醉开始时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "医院感染手术记录"."剂量单位" IS '麻醉药物剂量单位的机构内名称，如：mg，ml等';
COMMENT ON COLUMN "医院感染手术记录"."麻醉药物剂量" IS '使用麻醉药物的总量';
COMMENT ON COLUMN "医院感染手术记录"."麻醉药物名称" IS '麻醉药物在机构内编码体系中的名称';
COMMENT ON COLUMN "医院感染手术记录"."麻醉药物代码" IS '按照机构内编码规则赋予麻醉药物的唯一标识';
COMMENT ON COLUMN "医院感染手术记录"."麻醉执行科室名称" IS '麻醉执行的机构内名称';
COMMENT ON COLUMN "医院感染手术记录"."麻醉执行科室代码" IS '按照机构内编码规则赋予麻醉执行的唯一标识';
COMMENT ON COLUMN "医院感染手术记录"."麻醉分级名称" IS '麻醉分级代码在特定编码体系中的名称';
COMMENT ON COLUMN "医院感染手术记录"."ASA分级代码" IS '根据美同麻醉师协会(ASA)制定的分级标准，对病人体质状况和对手术危险性进行评估分级的结果在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染手术记录"."麻醉方法名称" IS '为患者进行手术、操作时使用的麻醉方法在特定编码体系中的名称';
COMMENT ON COLUMN "医院感染手术记录"."麻醉方法代码" IS '为患者进行手术、操作时使用的麻醉方法在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染手术记录"."麻醉者姓名" IS '对患者实施麻醉的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医院感染手术记录"."助手姓名II" IS '协助手术者完成手术及操作的第2助手在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医院感染手术记录"."助手姓名I" IS '协助手术者完成手术及操作的第1助手在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医院感染手术记录"."手术者姓名" IS '实施手术的主要执行人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "医院感染手术记录"."手术者工号" IS '实施手术的主要执行人员在原始特定编码体系中的编号';
COMMENT ON COLUMN "医院感染手术记录"."术中用药" IS '对患者术中用药情况的描述';
COMMENT ON COLUMN "医院感染手术记录"."术前用药" IS '对患者术前用药情况的描述';
COMMENT ON COLUMN "医院感染手术记录"."输液量(mL)" IS '总输液量，单位ml';
COMMENT ON COLUMN "医院感染手术记录"."输血(mL)" IS '输入红细胞、血小板、血浆、全血等的数量，剂量单位为mL';
COMMENT ON COLUMN "医院感染手术记录"."输血反应标志" IS '标志患者术中输血后是否发生了输血反应的标志';
COMMENT ON COLUMN "医院感染手术记录"."出血量(mL)" IS '手术过程的总出血量，单位ml';
COMMENT ON COLUMN "医院感染手术记录"."放置部位" IS '引流管放置在病人体内的具体位置的描述';
COMMENT ON COLUMN "医院感染手术记录"."引流材料数目" IS '对手术中引流材料数目的具体描述';
COMMENT ON COLUMN "医院感染手术记录"."引流材料名称" IS '对手术中引流材料名称的具体描述';
COMMENT ON COLUMN "医院感染手术记录"."引流标志" IS '标志术中是否有引流的标志';
COMMENT ON COLUMN "医院感染手术记录"."介入物名称" IS '实施手术操作时使用/放置的材料/药物的名称';
COMMENT ON COLUMN "医院感染手术记录"."手术切口类别名称" IS '手术切口类别的分类(如0类切口、I类切口、II类切口、III类切口)在特定编码体系中的名称';
COMMENT ON COLUMN "医院感染手术记录"."手术切口类别代码" IS '手术切口类别的分类(如0类切口、I类切口、II类切口、III类切口)在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染手术记录"."皮肤消毒描述" IS '对手术中皮肤消毒情况的具体描述';
COMMENT ON COLUMN "医院感染手术记录"."手术史标志" IS '标识患者有无手术经历的标志';
COMMENT ON COLUMN "医院感染手术记录"."手术过程描述" IS '手术过程的详细描述';
COMMENT ON COLUMN "医院感染手术记录"."手术体位名称" IS '手术时患者采取的体位在特定编码体系中的名称，如仰卧位、俯卧位、左侧卧位、截石位、屈氏位等';
COMMENT ON COLUMN "医院感染手术记录"."手术体位代码" IS '手术时为患者采取的体位(如仰卧位、俯卧位、左侧卧位、截石位、屈氏位等)在特定编码体系中的代码';
COMMENT ON COLUMN "医院感染手术记录"."手术部位名称" IS '实施手术的人体目标部位的机构内名称，如双侧鼻孔、臀部、左臂、右眼等';
COMMENT ON COLUMN "医院感染手术记录"."手术操作名称" IS '实施的手术及操作在机构内编码体系中的名称';
COMMENT ON COLUMN "医院感染手术记录"."手术操作代码" IS '按照机构内编码规则赋予实施的手术及操作的唯一标识';
COMMENT ON COLUMN "医院感染手术记录"."手术级别名称" IS '按照手术分级管理制度，根据风险性和难易程度不同划分的手术级别在特定编码体系中的名称，如一级手术、二级手术、三级手术、四级手术';
CREATE TABLE IF NOT EXISTS "医院感染手术记录" (
"手术级别代码" varchar (4) DEFAULT NULL,
 "手术结束时间" timestamp DEFAULT NULL,
 "手术开始时间" timestamp DEFAULT NULL,
 "手术间编号" varchar (20) DEFAULT NULL,
 "术前诊断名称" varchar (200) DEFAULT NULL,
 "术前诊断代码" varchar (100) DEFAULT NULL,
 "手术序列号" varchar (16) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "Rh血型名称" varchar (50) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "ABO血型名称" varchar (50) DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "年龄(月)" varchar (8) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "感染记录编号" varchar (36) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "医院感染手术记录编号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "文本内容" text,
 "择取消手术标志" varchar (1) DEFAULT NULL,
 "择期手术标志" varchar (1) DEFAULT NULL,
 "预防使用抗菌药物天数" decimal (4,
 0) DEFAULT NULL,
 "预防使用抗菌药物标志" varchar (1) DEFAULT NULL,
 "择期标志" varchar (1) DEFAULT NULL,
 "重返手术标志" varchar (1) DEFAULT NULL,
 "术后诊断名称" varchar (100) DEFAULT NULL,
 "术后诊断代码" varchar (64) DEFAULT NULL,
 "手术切口愈合等级名称" varchar (30) DEFAULT NULL,
 "手术切口愈合等级代码" varchar (5) DEFAULT NULL,
 "出复苏室时间" timestamp DEFAULT NULL,
 "入复苏室时间" timestamp DEFAULT NULL,
 "麻醉合并症描述" varchar (200) DEFAULT NULL,
 "麻醉合并症标志" varchar (1) DEFAULT NULL,
 "麻醉结束时间" timestamp DEFAULT NULL,
 "麻醉开始时间" timestamp DEFAULT NULL,
 "剂量单位" varchar (20) DEFAULT NULL,
 "麻醉药物剂量" varchar (10) DEFAULT NULL,
 "麻醉药物名称" varchar (250) DEFAULT NULL,
 "麻醉药物代码" varchar (50) DEFAULT NULL,
 "麻醉执行科室名称" varchar (100) DEFAULT NULL,
 "麻醉执行科室代码" varchar (20) DEFAULT NULL,
 "麻醉分级名称" varchar (50) DEFAULT NULL,
 "ASA分级代码" decimal (1,
 0) DEFAULT NULL,
 "麻醉方法名称" varchar (100) DEFAULT NULL,
 "麻醉方法代码" varchar (20) DEFAULT NULL,
 "麻醉者姓名" varchar (50) DEFAULT NULL,
 "助手姓名II" varchar (50) DEFAULT NULL,
 "助手姓名I" varchar (50) DEFAULT NULL,
 "手术者姓名" varchar (50) DEFAULT NULL,
 "手术者工号" varchar (20) DEFAULT NULL,
 "术中用药" varchar (100) DEFAULT NULL,
 "术前用药" varchar (100) DEFAULT NULL,
 "输液量(mL)" decimal (5,
 0) DEFAULT NULL,
 "输血(mL)" decimal (4,
 0) DEFAULT NULL,
 "输血反应标志" varchar (1) DEFAULT NULL,
 "出血量(mL)" decimal (5,
 0) DEFAULT NULL,
 "放置部位" varchar (50) DEFAULT NULL,
 "引流材料数目" varchar (200) DEFAULT NULL,
 "引流材料名称" varchar (200) DEFAULT NULL,
 "引流标志" varchar (1) DEFAULT NULL,
 "介入物名称" varchar (100) DEFAULT NULL,
 "手术切口类别名称" varchar (50) DEFAULT NULL,
 "手术切口类别代码" varchar (2) DEFAULT NULL,
 "皮肤消毒描述" varchar (200) DEFAULT NULL,
 "手术史标志" varchar (1) DEFAULT NULL,
 "手术过程描述" text,
 "手术体位名称" varchar (20) DEFAULT NULL,
 "手术体位代码" varchar (3) DEFAULT NULL,
 "手术部位名称" varchar (50) DEFAULT NULL,
 "手术操作名称" varchar (64) DEFAULT NULL,
 "手术操作代码" varchar (20) DEFAULT NULL,
 "手术级别名称" varchar (6) DEFAULT NULL,
 CONSTRAINT "医院感染手术记录"_"医院感染手术记录编号"_"医疗机构代码"_PK PRIMARY KEY ("医院感染手术记录编号",
 "医疗机构代码")
);


COMMENT ON TABLE "卫生监督协管信息报告登记表" IS '卫生监督协管信息登记，包括发现时间、信息类别、信息内容等';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."序号" IS '按照某一特定规则赋予登记信息在监督协管信息报告登记表单中的顺序号';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."发现时间" IS '监督协管发现问题当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."信息类别代码" IS '卫生计生监督协管的信息类别(如食源性疾病、饮用水卫生等)在特定编码体系中的代码';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."信息类别名称" IS '卫生计生监督协管的信息类别(如食源性疾病、饮用水卫生等)在特定编码体系中的名称';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."信息内容" IS '监督协管信息内容的详细描述';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."报告时间" IS '监督协管当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."报告人姓名" IS '报告人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."报告人工号" IS '报告人员的工号';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."报告流水号" IS '按照一定编码赋予报告卡的顺序号';
COMMENT ON COLUMN "卫生监督协管信息报告登记表"."机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
CREATE TABLE IF NOT EXISTS "卫生监督协管信息报告登记表" (
"机构名称" varchar (200) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "序号" varchar (50) DEFAULT NULL,
 "发现时间" timestamp DEFAULT NULL,
 "信息类别代码" varchar (2) DEFAULT NULL,
 "信息类别名称" varchar (2) DEFAULT NULL,
 "信息内容" varchar (500) DEFAULT NULL,
 "报告时间" timestamp DEFAULT NULL,
 "报告人姓名" varchar (50) DEFAULT NULL,
 "报告人工号" varchar (20) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "报告流水号" varchar (64) NOT NULL,
 "机构代码" varchar (22) NOT NULL,
 CONSTRAINT "卫生监督协管信息报告登记表"_"报告流水号"_"机构代码"_PK PRIMARY KEY ("报告流水号",
 "机构代码")
);


COMMENT ON TABLE "口腔颌面部肿瘤颅颌联合根治术记录" IS '口腔颌面部肿瘤颅颌联合根治术记录，包括肿瘤原发部位、手术方式、术后探查、并发症、抢救信息';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."抢救次数" IS '患者经历抢救的次数，计量单位为次';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."抢救成功次数" IS '患者抢救成功的次数，计量单位为次';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."主刀医生工号" IS '实施手术的主要执行人员在原始特定编码体系中的编号';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."主刀医生姓名" IS '实施手术的主要执行人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."填表人工号" IS '填表人在原始特定编码体系中的编号';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."填表人姓名" IS '填表人在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."填表时间" IS '完成填表时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."住院次数" IS '此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."出生日期" IS '患者出生当日的公元纪年日期';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."病床号" IS '患者入院时，所住床位对应的编号';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."病房号" IS '患者入院时，所住病房对应的编号';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."病区名称" IS '患者入院时，所住病区名称';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."就诊科室名称" IS '所在科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."就诊科室代码" IS '按照机构内编码规则赋予所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."手术申请单号" IS '按照某一特定编码规则赋予手术申请单的唯一标识';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."手术记录流水号" IS '按照某一特定编码规则赋予手术记录的唯一标识';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."联合根治术记录编号" IS '按照某一特定编码规则赋予联合根治术记录的唯一标识';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."ABO血型代码" IS '受检者按照ABO血型系统决定的血型在特定编码体系中的代码';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."Rh血型代码" IS '进行血型检查明确，或既往病历资料能够明确的患者Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."术前主要诊断代码" IS '术前主要诊断在《疾病分类及手术__疾病分类代码国家临床版_2.0》中的代码';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."术前主要诊断名称" IS '术前主要诊断在《疾病分类及手术_ 疾病分类代码国家临床版 2.0》中的名称';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."最终主要诊断代码" IS '最终主要诊断在《疾病分类及手术__疾病分类代码国家临床版_2.0》中的代码';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."最终主要诊断名称" IS '最终主要诊断在《疾病分类及手术_ 疾病分类代码国家临床版 2.0》中的名称';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."转归情况" IS '转归情况的分类代码';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."肿瘤原发部位" IS '肿瘤原发部位的分类代码';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."肿瘤其他原发部位" IS '肿瘤其他原发部位的详细描述';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."侵犯颅底骨" IS '侵犯颅底骨的分类代码';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."手术方式代码" IS '手术方式在特定编码体系中的代码';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."手术方式名称" IS '手术方式在特定编码体系中的名称';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."手术方式说明" IS '手术操作方式详细说明';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."口腔颌面修复方式" IS '修复方式的分类代码';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."带蒂皮瓣成活标志" IS '标识带蒂皮瓣是否成活的标志';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."游离皮瓣成活标志" IS '标识游离皮瓣是否成活的标志';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."手术开始时间" IS '以手术操作刀碰皮(切皮)时点计算，经自然腔道到达手术部位，没有皮肤切口的手术，开始时间以手术器械进入人体腔道的时点为准,对患者开始手术操作时的公元纪年日期和时间的完整描述。';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."手术结束时间" IS '对患者结束手术操作时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."出院时间" IS '患者实际办理出院手续时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."术后手术部位感染标志" IS '患者有无术后手术部位感染的情况标识';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."术后血肿手术探查标志" IS '患者有无术后血肿手术探查的情况标识';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."血肿探查次数" IS '血肿探查次数的具体数值';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."皮瓣术后血管危急探查标志" IS '患者有无皮瓣术后血管危急探查的情况标识';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."血管危象探查次数" IS '血管危象探查次数的具体数值';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."术后脑脊液漏标志" IS '患者有无术后脑脊液漏的情况标识';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."术后颅内感染标志" IS '患者有无术后颅内感染的情况标识';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."并发症" IS '患者并发症的分类代码';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."其他并发症" IS '患者其他并发症详细描述';
COMMENT ON COLUMN "口腔颌面部肿瘤颅颌联合根治术记录"."术后抢救标志" IS '患者有无术后抢救的情况标识';
CREATE TABLE IF NOT EXISTS "口腔颌面部肿瘤颅颌联合根治术记录" (
"抢救次数" decimal (5,
 0) DEFAULT NULL,
 "抢救成功次数" decimal (5,
 0) DEFAULT NULL,
 "主刀医生工号" varchar (20) DEFAULT NULL,
 "主刀医生姓名" varchar (50) DEFAULT NULL,
 "填表人工号" varchar (20) DEFAULT NULL,
 "填表人姓名" varchar (50) DEFAULT NULL,
 "填表时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "手术申请单号" varchar (32) DEFAULT NULL,
 "手术记录流水号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "联合根治术记录编号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "术前主要诊断代码" varchar (64) DEFAULT NULL,
 "术前主要诊断名称" varchar (100) DEFAULT NULL,
 "最终主要诊断代码" varchar (20) DEFAULT NULL,
 "最终主要诊断名称" varchar (100) DEFAULT NULL,
 "转归情况" decimal (1,
 0) DEFAULT NULL,
 "肿瘤原发部位" decimal (1,
 0) DEFAULT NULL,
 "肿瘤其他原发部位" varchar (200) DEFAULT NULL,
 "侵犯颅底骨" decimal (1,
 0) DEFAULT NULL,
 "手术方式代码" varchar (20) DEFAULT NULL,
 "手术方式名称" varchar (100) DEFAULT NULL,
 "手术方式说明" varchar (200) DEFAULT NULL,
 "口腔颌面修复方式" decimal (1,
 0) DEFAULT NULL,
 "带蒂皮瓣成活标志" varchar (1) DEFAULT NULL,
 "游离皮瓣成活标志" varchar (1) DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "手术开始时间" timestamp DEFAULT NULL,
 "手术结束时间" timestamp DEFAULT NULL,
 "出院时间" timestamp DEFAULT NULL,
 "术后手术部位感染标志" varchar (1) DEFAULT NULL,
 "术后血肿手术探查标志" varchar (1) DEFAULT NULL,
 "血肿探查次数" decimal (5,
 0) DEFAULT NULL,
 "皮瓣术后血管危急探查标志" varchar (1) DEFAULT NULL,
 "血管危象探查次数" decimal (5,
 0) DEFAULT NULL,
 "术后脑脊液漏标志" varchar (1) DEFAULT NULL,
 "术后颅内感染标志" varchar (1) DEFAULT NULL,
 "并发症" varchar (200) DEFAULT NULL,
 "其他并发症" varchar (200) DEFAULT NULL,
 "术后抢救标志" varchar (1) DEFAULT NULL,
 CONSTRAINT "口腔颌面部肿瘤颅颌联合根治术记录"_"联合根治术记录编号"_"医疗机构代码"_PK PRIMARY KEY ("联合根治术记录编号",
 "医疗机构代码")
);


COMMENT ON TABLE "同种异体运动系统结构性组织移植术记录" IS '同种异体运动系统结构性组织移植术记录，包括移植物类型、种类、材料面积、重量信息，移植物去除等';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."主刀医生姓名" IS '实施手术的主要执行人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."移植物类型" IS '离体24小时内的移植物、未经处理直接应用于手术的记录为新鲜、其余记录为处理';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."移植物种类" IS '移植物种类（如骨、软骨、韧带、肌腱等）在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."移植物材料面积(CM^2)" IS '移植物材料面积，计量单位为CM^2';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."移植物材料重量(g)" IS '移植物材料重量，计量单位为g';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."移植物材料长短尺寸(CM)" IS '移植物材料长短尺寸，计量单位为cm';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."手术感染标志" IS '指手术部位的临床感染或者有细菌学一句的感染';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."术后血栓形成标志" IS '术后有无血栓形成的情况标志';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."移植物去除标志" IS '标识是否移植物去除的标志';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."同种异体运动移植后影像学和电生理学评估优良率" IS '移植后影像学或电生理学评估优良、是指同种异体运动系统_结构性组织移植后，按骨关节各亚专科优良率评估标准，影像学或电生理学评估较术前同类检查优良';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."同种异体手术方式" IS '手术方式在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."微生物检出" IS '微生物检出情况代码';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."移植物来源" IS '移植物来源代码';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."填表人工号" IS '填表人在原始特定编码体系中的编号';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."填表人姓名" IS '填表人在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."填表时间" IS '完成填表时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."移植部位代码" IS '记录移植物移植的部位、累及多个部位的记录首要部位';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."移植术记录编号" IS '按照某一特定编码规则赋予移植术记录的唯一标识';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."手术记录流水号" IS '按照某一特定编码规则赋予手术记录的唯一标识';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."手术申请单号" IS '按照某一特定编码规则赋予手术申请单的唯一标识';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."就诊科室代码" IS '按照机构内编码规则赋予所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."就诊科室名称" IS '所在科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."病区名称" IS '患者入院时，所住病区名称';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."病房号" IS '患者入院时，所住病房对应的编号';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."病床号" IS '患者入院时，所住床位对应的编号';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."住院次数" IS '此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."出生日期" IS '患者出生当日的公元纪年日期';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."ABO血型代码" IS '受检者按照ABO血型系统决定的血型在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."Rh血型代码" IS '进行血型检查明确，或既往病历资料能够明确的患者Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."移植需求" IS '移植需求在特定编码体系中的代码';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."开始手术时间" IS '以手术操作刀碰皮(切皮)时点计算，经自然腔道到达手术部位，没有皮肤切口的手术，开始时间以手术器械进入人体腔道的时点为准,对患者开始手术操作时的公元纪年日期和时间的完整描述。';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."结束手术时间" IS '对患者结束手术操作时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "同种异体运动系统结构性组织移植术记录"."主刀医生工号" IS '实施手术的主要执行人员在原始特定编码体系中的编号';
CREATE TABLE IF NOT EXISTS "同种异体运动系统结构性组织移植术记录" (
"主刀医生姓名" varchar (50) DEFAULT NULL,
 "移植物类型" decimal (1,
 0) DEFAULT NULL,
 "移植物种类" decimal (1,
 0) DEFAULT NULL,
 "移植物材料面积(CM^2)" decimal (6,
 2) DEFAULT NULL,
 "移植物材料重量(g)" decimal (6,
 2) DEFAULT NULL,
 "移植物材料长短尺寸(CM)" decimal (6,
 2) DEFAULT NULL,
 "手术感染标志" varchar (1) DEFAULT NULL,
 "术后血栓形成标志" varchar (1) DEFAULT NULL,
 "移植物去除标志" varchar (1) DEFAULT NULL,
 "同种异体运动移植后影像学和电生理学评估优良率" decimal (1,
 0) DEFAULT NULL,
 "同种异体手术方式" varchar (30) DEFAULT NULL,
 "微生物检出" decimal (1,
 0) DEFAULT NULL,
 "移植物来源" decimal (1,
 0) DEFAULT NULL,
 "填表人工号" varchar (20) DEFAULT NULL,
 "填表人姓名" varchar (50) DEFAULT NULL,
 "填表时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "移植部位代码" decimal (1,
 0) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "移植术记录编号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "手术记录流水号" varchar (64) DEFAULT NULL,
 "手术申请单号" varchar (32) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "移植需求" decimal (1,
 0) DEFAULT NULL,
 "开始手术时间" timestamp DEFAULT NULL,
 "结束手术时间" timestamp DEFAULT NULL,
 "主刀医生工号" varchar (20) DEFAULT NULL,
 CONSTRAINT "同种异体运动系统结构性组织移植术记录"_"医疗机构代码"_"移植术记录编号"_PK PRIMARY KEY ("医疗机构代码",
 "移植术记录编号")
);


COMMENT ON TABLE "固定资产信息" IS '固定资产使用管理记录，包括固定资产位置、名称、类别、许可证、购买验收信息、使用情况、租借情况等';
COMMENT ON COLUMN "固定资产信息"."验收人姓名" IS '验收员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "固定资产信息"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "固定资产信息"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "固定资产信息"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "固定资产信息"."科室名称" IS '资产所在科室在特定编码体系中的名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "固定资产信息"."科室代码" IS '按照特定编码规则赋予资产所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "固定资产信息"."租借结束日期" IS '该固定资产停止租借当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "固定资产信息"."租借开始日期" IS '该固定资产开始租借当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "固定资产信息"."租借单位" IS '租借企业在工商局注册、审批通过后的厂家名称';
COMMENT ON COLUMN "固定资产信息"."租借标志" IS '本资产是否租借的标志';
COMMENT ON COLUMN "固定资产信息"."租借合同序号" IS '将资产进行租借时合同的编号';
COMMENT ON COLUMN "固定资产信息"."停用日期" IS '该固定资产停止使用当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "固定资产信息"."停用标志" IS '本资产是否停用的标志';
COMMENT ON COLUMN "固定资产信息"."放置地点" IS '固定资产放置位置的详细描述';
COMMENT ON COLUMN "固定资产信息"."固定资产使用状态" IS '资产使用情况(如启用、未启用、报废等)在特定编码体系中的名称';
COMMENT ON COLUMN "固定资产信息"."固定资产使用状态代码" IS '资产使用情况(如启用、未启用、报废等)在特定编码体系中的代码';
COMMENT ON COLUMN "固定资产信息"."车辆用途分类名称" IS '车辆用途分类(如公务、医疗等)在特定编码体系中的名称';
COMMENT ON COLUMN "固定资产信息"."车辆用途分类代码" IS '车辆用途分类(如公务、医疗等)在特定编码体系中的代码';
COMMENT ON COLUMN "固定资产信息"."固定资产用途分类" IS '固定资产用途（生产经营用或非生产经营用）在特定编码体系中的名称';
COMMENT ON COLUMN "固定资产信息"."固定资产用途分类代码" IS '固定资产用途（生产经营用或非生产经营用）在特定编码体系中的代码';
COMMENT ON COLUMN "固定资产信息"."己用年限" IS '资产已经使用的寿命，计量单位为年';
COMMENT ON COLUMN "固定资产信息"."旧设备标志" IS '本资产时是否为旧设备的标志';
COMMENT ON COLUMN "固定资产信息"."累计提折旧" IS '本资产的累计折旧金额，计量单位为人民币元';
COMMENT ON COLUMN "固定资产信息"."折旧标志" IS '本资产是否折旧的标志';
COMMENT ON COLUMN "固定资产信息"."进口标志" IS '本资产是否为进口的标志';
COMMENT ON COLUMN "固定资产信息"."使用年限" IS '资产的理论使用寿命，计量单位为年';
COMMENT ON COLUMN "固定资产信息"."预计残值" IS '本资产的固定资产预计残值估算，计量单位为人民币元';
COMMENT ON COLUMN "固定资产信息"."设备现值" IS '本资产的固定资产现值估算，计量单位为人民币元';
COMMENT ON COLUMN "固定资产信息"."设备原值" IS '本资产的固定资产原值估算，计量单位为人民币元，默认为0';
COMMENT ON COLUMN "固定资产信息"."设备单价" IS '市面上固定资产的单价，计量单位为元';
COMMENT ON COLUMN "固定资产信息"."固定资产质量等级" IS '质量等级(如一级、二级、三级等)在特定编码体系中的名称';
COMMENT ON COLUMN "固定资产信息"."固定资产质量等级代码" IS '质量等级(如一级、二级、三级等)在特定编码体系中的代码';
COMMENT ON COLUMN "固定资产信息"."固定资产性质" IS '固定资产性质(如自有固定资产、融资租入固定资产等)在特定编码体系中的名称';
COMMENT ON COLUMN "固定资产信息"."固定资产性质代码" IS '固定资产性质(如自有固定资产、融资租入固定资产等)在特定编码体系中的代码';
COMMENT ON COLUMN "固定资产信息"."开始使用日期" IS '该固定资产开始使用当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "固定资产信息"."验收日期" IS '该固定资产验收当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "固定资产信息"."购买日期" IS '该固定资产购买当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "固定资产信息"."固定资产采购资金来源" IS '资金来源(如自有资金、财政拨款等)在特定编码体系中的名称';
COMMENT ON COLUMN "固定资产信息"."固定资产采购资金来源代码" IS '资金来源(如自有资金、财政拨款等)在特定编码体系中的代码';
COMMENT ON COLUMN "固定资产信息"."免税标志" IS '购买时本资产时是否免税的标志';
COMMENT ON COLUMN "固定资产信息"."固定资产采购单价" IS '固定资产购买时的单价，计量单位为元';
COMMENT ON COLUMN "固定资产信息"."采购合同号" IS '采购时合同的编号';
COMMENT ON COLUMN "固定资产信息"."供应商名称" IS '供应商在工商局注册、审批通过后的厂家名称';
COMMENT ON COLUMN "固定资产信息"."采购员姓名" IS '采购员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "固定资产信息"."采购流水号" IS '按照某一特性编码规则赋予采购记录流水号唯一标志的顺序号';
COMMENT ON COLUMN "固定资产信息"."设备序列号" IS '设备出厂的系列号，是本设备的唯一性标志';
COMMENT ON COLUMN "固定资产信息"."固定资产产地国籍" IS '产地国籍在特定编码体系中的名称';
COMMENT ON COLUMN "固定资产信息"."固定资产产地国籍代码" IS '产地国籍在特定编码体系中的代码';
COMMENT ON COLUMN "固定资产信息"."出厂日期" IS '该固定资产出厂当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "固定资产信息"."生产厂家名称" IS '固定资产生产企业在工商局注册、审批通过后的厂家名称';
COMMENT ON COLUMN "固定资产信息"."品牌" IS '不同厂家的固定资产识别标志';
COMMENT ON COLUMN "固定资产信息"."计量单位" IS '固定资产计量单位的机构内名称';
COMMENT ON COLUMN "固定资产信息"."型号" IS '检查设备的出厂型号，如梅里埃vitek32、BD';
COMMENT ON COLUMN "固定资产信息"."规格" IS '医用设备的规格描述';
COMMENT ON COLUMN "固定资产信息"."固定资产分类" IS '固定资产分类(如园林、住宅用地、海域等)在特定编码体系中的名称';
COMMENT ON COLUMN "固定资产信息"."固定资产分类代码" IS '固定资产分类(如园林、住宅用地、海域等)在GB/T14885-2010中的代码';
COMMENT ON COLUMN "固定资产信息"."甲乙类设备资产许可证号" IS '固定资产属于甲乙类大型设备时对应的许可证编号';
COMMENT ON COLUMN "固定资产信息"."大型设备管理类别" IS '大型设备管理类别(如甲类、乙类等)在特定编码体系中的名称';
COMMENT ON COLUMN "固定资产信息"."大型设备管理类别代码" IS '大型设备管理类别(如甲类、乙类等)在特定编码体系中的代码';
COMMENT ON COLUMN "固定资产信息"."固定资产名称" IS '固定资产的名称';
COMMENT ON COLUMN "固定资产信息"."固定资产所在位置类型" IS '固定资产所在位置类型(如仓库、科室等)在特定编码体系中的名称';
COMMENT ON COLUMN "固定资产信息"."固定资产所在位置类型代码" IS '固定资产所在位置类型(如仓库、科室等)在特定编码体系中的代码';
COMMENT ON COLUMN "固定资产信息"."卡片状态" IS '固定资产卡片状态(如未审核、审核、确认、处置完毕等)在特定编码体系中的名称';
COMMENT ON COLUMN "固定资产信息"."卡片状态代码" IS '固定资产卡片状态(如未审核、审核、确认、处置完毕等)在特定编码体系中的代码';
COMMENT ON COLUMN "固定资产信息"."固定资产卡片号" IS '按照某一特定编码规则赋予固定资产卡片的唯一性编号';
COMMENT ON COLUMN "固定资产信息"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "固定资产信息"."固定资产记录编号" IS '按照某一特定编码规则赋予固定资产记录的顺序号，是固定资产记录的唯一标识';
COMMENT ON COLUMN "固定资产信息"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "固定资产信息"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
CREATE TABLE IF NOT EXISTS "固定资产信息" (
"验收人姓名" varchar (50) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "租借结束日期" date DEFAULT NULL,
 "租借开始日期" date DEFAULT NULL,
 "租借单位" varchar (700) DEFAULT NULL,
 "租借标志" varchar (1) DEFAULT NULL,
 "租借合同序号" varchar (700) DEFAULT NULL,
 "停用日期" date DEFAULT NULL,
 "停用标志" varchar (1) DEFAULT NULL,
 "放置地点" varchar (700) DEFAULT NULL,
 "固定资产使用状态" varchar (100) DEFAULT NULL,
 "固定资产使用状态代码" varchar (10) DEFAULT NULL,
 "车辆用途分类名称" varchar (100) DEFAULT NULL,
 "车辆用途分类代码" varchar (10) DEFAULT NULL,
 "固定资产用途分类" varchar (100) DEFAULT NULL,
 "固定资产用途分类代码" varchar (10) DEFAULT NULL,
 "己用年限" decimal (5,
 0) DEFAULT NULL,
 "旧设备标志" varchar (1) DEFAULT NULL,
 "累计提折旧" varchar (50) DEFAULT NULL,
 "折旧标志" varchar (1) DEFAULT NULL,
 "进口标志" varchar (1) DEFAULT NULL,
 "使用年限" varchar (8) DEFAULT NULL,
 "预计残值" varchar (50) DEFAULT NULL,
 "设备现值" varchar (50) DEFAULT NULL,
 "设备原值" varchar (50) DEFAULT NULL,
 "设备单价" varchar (50) DEFAULT NULL,
 "固定资产质量等级" varchar (100) DEFAULT NULL,
 "固定资产质量等级代码" varchar (10) DEFAULT NULL,
 "固定资产性质" varchar (100) DEFAULT NULL,
 "固定资产性质代码" varchar (10) DEFAULT NULL,
 "开始使用日期" date DEFAULT NULL,
 "验收日期" date DEFAULT NULL,
 "购买日期" date DEFAULT NULL,
 "固定资产采购资金来源" varchar (100) DEFAULT NULL,
 "固定资产采购资金来源代码" varchar (10) DEFAULT NULL,
 "免税标志" varchar (1) DEFAULT NULL,
 "固定资产采购单价" varchar (50) DEFAULT NULL,
 "采购合同号" varchar (100) DEFAULT NULL,
 "供应商名称" varchar (700) DEFAULT NULL,
 "采购员姓名" varchar (50) DEFAULT NULL,
 "采购流水号" varchar (700) DEFAULT NULL,
 "设备序列号" varchar (700) DEFAULT NULL,
 "固定资产产地国籍" varchar (700) DEFAULT NULL,
 "固定资产产地国籍代码" varchar (100) DEFAULT NULL,
 "出厂日期" date DEFAULT NULL,
 "生产厂家名称" varchar (70) DEFAULT NULL,
 "品牌" varchar (700) DEFAULT NULL,
 "计量单位" varchar (100) DEFAULT NULL,
 "型号" varchar (300) DEFAULT NULL,
 "规格" varchar (700) DEFAULT NULL,
 "固定资产分类" varchar (100) DEFAULT NULL,
 "固定资产分类代码" varchar (20) DEFAULT NULL,
 "甲乙类设备资产许可证号" varchar (100) DEFAULT NULL,
 "大型设备管理类别" varchar (100) DEFAULT NULL,
 "大型设备管理类别代码" varchar (10) DEFAULT NULL,
 "固定资产名称" varchar (100) DEFAULT NULL,
 "固定资产所在位置类型" varchar (100) DEFAULT NULL,
 "固定资产所在位置类型代码" varchar (10) DEFAULT NULL,
 "卡片状态" varchar (100) DEFAULT NULL,
 "卡片状态代码" varchar (10) DEFAULT NULL,
 "固定资产卡片号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "固定资产记录编号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 CONSTRAINT "固定资产信息"_"固定资产记录编号"_"医疗机构代码"_PK PRIMARY KEY ("固定资产记录编号",
 "医疗机构代码")
);


COMMENT ON TABLE "基本公卫日结数据_区域(累计)" IS '基本公卫区域累计日结数据，如建档人数、高血压患者规范管理人数等';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."区划代码" IS '医疗机构所在区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."辖区内电子健康档案被调用的份数" IS '截止统计日期，该行政区划辖区电子健康档案被调用的份数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."新生儿访视人数" IS '截止统计日期，该行政区划辖区新生儿访视人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."产后访视数" IS '截止统计日期，该行政区划辖区产后访视人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."孕13周之前建册并进行第一次产前检查的产妇人数" IS '截止统计日期，该行政区划辖区孕13周之前建册并进行第一次产前检查的产妇人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."肺结核患者健康管理人数" IS '截止统计日期，该行政区划辖区肺结核患者健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."登记在册的严重精神障碍患者人数" IS '截止统计日期，该行政区划辖区登记在册的严重精神障碍患者人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."严重精神障碍患者规范管理人数" IS '截止统计日期，该行政区划辖区严重精神障碍患者规范管理人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."最近一次随访空腹血糖达标人数" IS '截止统计日期，该行政区划辖区最近一次随访空腹血糖达标人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."糖尿病患者规范管理人数" IS '截止统计日期，该行政区划辖区糖尿病患者规范管理人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."糖尿病患者健康管理人数" IS '截止统计日期，该行政区划辖区糖尿病患者健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."最近一次随访血压达标人数" IS '截止统计日期，该行政区划辖区最近一次随访血压达标人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."高血压患者规范管理人数" IS '截止统计日期，该行政区划辖区高血压患者规范管理人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."高血压患者健康管理人数" IS '截止统计日期，该行政区划辖区高血压患者健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."65岁及以上老年人中医药健康管理人数" IS '截止统计日期，该行政区划辖区65岁及以上老年人中医药健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."65岁及以上老年人健康管理人数" IS '截止统计日期，该行政区划辖区65岁及以上老年人健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."0～3岁儿童中医药健康管理人数" IS '截止统计日期，该行政区划辖区0～3岁儿童中医药健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."0～6岁儿童健康管理人数" IS '截止统计日期，该行政区划辖区0～6岁儿童健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."建立健康档案人数" IS '截止统计日期，该行政区划辖区建立健康档案的总人数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."有动态记录的档案份数" IS '截止统计日期，该行政区划辖区有动态记录的档案份数';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."统计日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "基本公卫日结数据_区域(累计)"."区划名称" IS '医疗机构所在区划在机构内编码体系中的名称';
CREATE TABLE IF NOT EXISTS "基本公卫日结数据_区域(
累计)" ("区划代码" varchar (12) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "辖区内电子健康档案被调用的份数" decimal (10,
 0) DEFAULT NULL,
 "新生儿访视人数" decimal (10,
 0) DEFAULT NULL,
 "产后访视数" decimal (10,
 0) DEFAULT NULL,
 "孕13周之前建册并进行第一次产前检查的产妇人数" decimal (10,
 0) DEFAULT NULL,
 "肺结核患者健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "登记在册的严重精神障碍患者人数" decimal (10,
 0) DEFAULT NULL,
 "严重精神障碍患者规范管理人数" decimal (10,
 0) DEFAULT NULL,
 "最近一次随访空腹血糖达标人数" decimal (10,
 0) DEFAULT NULL,
 "糖尿病患者规范管理人数" decimal (10,
 0) DEFAULT NULL,
 "糖尿病患者健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "最近一次随访血压达标人数" decimal (10,
 0) DEFAULT NULL,
 "高血压患者规范管理人数" decimal (10,
 0) DEFAULT NULL,
 "高血压患者健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "65岁及以上老年人中医药健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "65岁及以上老年人健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "0～3岁儿童中医药健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "0～6岁儿童健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "建立健康档案人数" decimal (10,
 0) DEFAULT NULL,
 "有动态记录的档案份数" decimal (10,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "统计日期" date NOT NULL,
 "区划名称" varchar (12) NOT NULL,
 CONSTRAINT "基本公卫日结数据_区域(累计)"_"区划代码"_"统计日期"_"区划名称"_PK PRIMARY KEY ("区划代码",
 "统计日期",
 "区划名称")
);


COMMENT ON TABLE "基本公卫日结数据_机构(累计)" IS '基本公卫机构累计日结数据，如建档人数、高血压患者规范管理人数等';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."糖尿病患者健康管理人数" IS '截止统计日期，该机构内糖尿病患者健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."糖尿病患者规范管理人数" IS '截止统计日期，该机构内糖尿病患者规范管理人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."辖区内电子健康档案被调用的份数" IS '截止统计日期，该机构内电子健康档案被调用的份数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."新生儿访视人数" IS '截止统计日期，该机构内新生儿访视人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."产后访视数" IS '截止统计日期，该机构内产后访视人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."孕13周之前建册并进行第一次产前检查的产妇人数" IS '截止统计日期，该机构内孕13周之前建册并进行第一次产前检查的产妇人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."肺结核患者健康管理人数" IS '截止统计日期，该机构内肺结核患者健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."登记在册的严重精神障碍患者人数" IS '截止统计日期，该机构内登记在册的严重精神障碍患者人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."严重精神障碍患者规范管理人数" IS '截止统计日期，该机构内严重精神障碍患者规范管理人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."最近一次随访空腹血糖达标人数" IS '截止统计日期，该机构内最近一次随访空腹血糖达标人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."统计日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."有动态记录的档案份数" IS '截止统计日期，该机构内有动态记录的档案份数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."建立健康档案人数" IS '截止统计日期，该机构内建立健康档案的总人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."0～6岁儿童健康管理人数" IS '截止统计日期，该机构内0～6岁儿童健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."0～3岁儿童中医药健康管理人数" IS '截止统计日期，该机构内0～3岁儿童中医药健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."65岁及以上老年人健康管理人数" IS '截止统计日期，该机构内65岁及以上老年人健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."65岁及以上老年人中医药健康管理人数" IS '截止统计日期，该机构内65岁及以上老年人中医药健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."高血压患者健康管理人数" IS '截止统计日期，该机构内高血压患者健康管理人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."高血压患者规范管理人数" IS '截止统计日期，该机构内高血压患者规范管理人数';
COMMENT ON COLUMN "基本公卫日结数据_机构(累计)"."最近一次随访血压达标人数" IS '截止统计日期，该机构内最近一次随访血压达标人数';
CREATE TABLE IF NOT EXISTS "基本公卫日结数据_机构(
累计)" ("糖尿病患者健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "糖尿病患者规范管理人数" decimal (10,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "辖区内电子健康档案被调用的份数" decimal (10,
 0) DEFAULT NULL,
 "新生儿访视人数" decimal (10,
 0) DEFAULT NULL,
 "产后访视数" decimal (10,
 0) DEFAULT NULL,
 "孕13周之前建册并进行第一次产前检查的产妇人数" decimal (10,
 0) DEFAULT NULL,
 "肺结核患者健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "登记在册的严重精神障碍患者人数" decimal (10,
 0) DEFAULT NULL,
 "严重精神障碍患者规范管理人数" decimal (10,
 0) DEFAULT NULL,
 "最近一次随访空腹血糖达标人数" decimal (10,
 0) DEFAULT NULL,
 "机构代码" varchar (22) NOT NULL,
 "机构名称" varchar (200) DEFAULT NULL,
 "统计日期" date NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "有动态记录的档案份数" decimal (10,
 0) DEFAULT NULL,
 "建立健康档案人数" decimal (10,
 0) DEFAULT NULL,
 "0～6岁儿童健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "0～3岁儿童中医药健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "65岁及以上老年人健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "65岁及以上老年人中医药健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "高血压患者健康管理人数" decimal (10,
 0) DEFAULT NULL,
 "高血压患者规范管理人数" decimal (10,
 0) DEFAULT NULL,
 "最近一次随访血压达标人数" decimal (10,
 0) DEFAULT NULL,
 CONSTRAINT "基本公卫日结数据_机构(累计)"_"机构代码"_"统计日期"_PK PRIMARY KEY ("机构代码",
 "统计日期")
);


COMMENT ON TABLE "基本数字数据表" IS '基本数字项目数据统计表，包括基本数字序号、账套编码、会计年度、月度、基本数字项目编码、项目数据等';
COMMENT ON COLUMN "基本数字数据表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本数字数据表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本数字数据表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "基本数字数据表"."业务数据更新时间" IS '业务数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本数字数据表"."业务数据新建时间" IS '业务数据新建时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "基本数字数据表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "基本数字数据表"."基本数字项目数据" IS '基本数字项目的计量值';
COMMENT ON COLUMN "基本数字数据表"."基本数字项目代码" IS '基本数字项目(如平均开放床位、在职职工人数等)在机构内编码体系中的代码';
COMMENT ON COLUMN "基本数字数据表"."会计月份" IS '会计统计月份的描述，格式为MM';
COMMENT ON COLUMN "基本数字数据表"."会计年度" IS '会计统计年份的描述，格式为YYYY';
COMMENT ON COLUMN "基本数字数据表"."账套代码" IS '用于资产管理的唯一性编码';
COMMENT ON COLUMN "基本数字数据表"."基本数字序号" IS '按照某一特定编码规则赋予基本数字记录的顺序号，是基本数字记录的唯一标识';
COMMENT ON COLUMN "基本数字数据表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "基本数字数据表"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
CREATE TABLE IF NOT EXISTS "基本数字数据表" (
"数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "业务数据更新时间" timestamp DEFAULT NULL,
 "业务数据新建时间" timestamp DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "基本数字项目数据" decimal (20,
 4) DEFAULT NULL,
 "基本数字项目代码" varchar (20) NOT NULL,
 "会计月份" varchar (2) NOT NULL,
 "会计年度" varchar (4) NOT NULL,
 "账套代码" varchar (30) NOT NULL,
 "基本数字序号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 CONSTRAINT "基本数字数据表"_"基本数字项目代码"_"会计月份"_"会计年度"_"账套代码"_"基本数字序号"_"医疗机构代码"_PK PRIMARY KEY ("基本数字项目代码",
 "会计月份",
 "会计年度",
 "账套代码",
 "基本数字序号",
 "医疗机构代码")
);


COMMENT ON TABLE "家庭医生签约日结数据_区域(周期)" IS '家庭医生区域签约周期日结数据，如签约总人数、重点人群签约数等';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."下转回访的签约居民人数" IS '统计周期内，该行政区划辖区下转回访的签约居民人数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."签约协议完整的人数" IS '统计周期内，该行政区划辖区签约协议完整的人数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."家庭医生重点人群签约数" IS '统计周期内，该行政区划辖区家庭医生重点人群签约数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."家庭医生签约总人数" IS '统计周期内，该行政区划辖区家庭医生签约总人数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."统计日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."区划名称" IS '医疗机构所在区划在机构内编码体系中的名称';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."区划代码" IS '医疗机构所在区划在机构内编码体系中的唯一标识';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."下转的签约居民总数" IS '统计周期内，该行政区划辖区下转的签约居民总数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."签约居民至其签约医生就诊次数" IS '统计周期内，该行政区划辖区签约居民至其签约医生就诊次数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."签约居民就诊人次数" IS '统计周期内，该行政区划辖区签约居民就诊人次数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."签约居民定点机构就诊人次数" IS '统计周期内，该行政区划辖区签约居民定点机构就诊人次数';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."公共卫生服务经费金额" IS '统计周期内，该行政区划辖区公共卫生服务经费金额';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."财政投入金额" IS '统计周期内，该行政区划辖区财政投入金额';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."医保基金金额" IS '统计周期内，该行政区划辖区医保基金金额';
COMMENT ON COLUMN "家庭医生签约日结数据_区域(周期)"."家庭医生签约服务费" IS '统计周期内，该行政区划辖区家庭医生签约服务费';
CREATE TABLE IF NOT EXISTS "家庭医生签约日结数据_区域(
周期)" ("下转回访的签约居民人数" decimal (10,
 0) DEFAULT NULL,
 "签约协议完整的人数" decimal (10,
 0) DEFAULT NULL,
 "家庭医生重点人群签约数" decimal (10,
 0) DEFAULT NULL,
 "家庭医生签约总人数" decimal (10,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "统计日期" date NOT NULL,
 "区划名称" varchar (12) NOT NULL,
 "区划代码" varchar (12) NOT NULL,
 "下转的签约居民总数" decimal (10,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "签约居民至其签约医生就诊次数" decimal (10,
 0) DEFAULT NULL,
 "签约居民就诊人次数" decimal (10,
 0) DEFAULT NULL,
 "签约居民定点机构就诊人次数" decimal (10,
 0) DEFAULT NULL,
 "公共卫生服务经费金额" decimal (10,
 2) DEFAULT NULL,
 "财政投入金额" decimal (10,
 2) DEFAULT NULL,
 "医保基金金额" decimal (10,
 2) DEFAULT NULL,
 "家庭医生签约服务费" decimal (10,
 2) DEFAULT NULL,
 CONSTRAINT "家庭医生签约日结数据_区域(周期)"_"统计日期"_"区划名称"_"区划代码"_PK PRIMARY KEY ("统计日期",
 "区划名称",
 "区划代码")
);


COMMENT ON TABLE "家庭医生签约日结数据_机构(周期)" IS '家庭医生机构签约周期日结数据，如签约总人数、重点人群签约数等';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."医保基金金额" IS '截止统计日期，该机构医保基金金额';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."家庭医生签约服务费" IS '截止统计日期，该机构家庭医生签约服务费';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."下转的签约居民总数" IS '截止统计日期，该机构下转的签约居民总数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."下转回访的签约居民人数" IS '截止统计日期，该机构下转回访的签约居民人数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."签约协议完整的人数" IS '截止统计日期，该机构签约协议完整的人数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."家庭医生重点人群签约数" IS '截止统计日期，该机构家庭医生重点人群签约数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."家庭医生签约总人数" IS '截止统计日期，该机构家庭医生签约总人数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."统计日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."签约居民至其签约医生就诊次数" IS '截止统计日期，该机构签约居民至其签约医生就诊次数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."签约居民就诊人次数" IS '截止统计日期，该机构签约居民就诊人次数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."签约居民定点机构就诊人次数" IS '截止统计日期，该机构签约居民定点机构就诊人次数';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."公共卫生服务经费金额" IS '截止统计日期，该机构公共卫生服务经费金额';
COMMENT ON COLUMN "家庭医生签约日结数据_机构(周期)"."财政投入金额" IS '截止统计日期，该机构财政投入金额';
CREATE TABLE IF NOT EXISTS "家庭医生签约日结数据_机构(
周期)" ("医保基金金额" decimal (10,
 2) DEFAULT NULL,
 "家庭医生签约服务费" decimal (10,
 2) DEFAULT NULL,
 "下转的签约居民总数" decimal (10,
 0) DEFAULT NULL,
 "下转回访的签约居民人数" decimal (10,
 0) DEFAULT NULL,
 "签约协议完整的人数" decimal (10,
 0) DEFAULT NULL,
 "家庭医生重点人群签约数" decimal (10,
 0) DEFAULT NULL,
 "家庭医生签约总人数" decimal (10,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "统计日期" date NOT NULL,
 "机构名称" varchar (200) DEFAULT NULL,
 "机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "签约居民至其签约医生就诊次数" decimal (10,
 0) DEFAULT NULL,
 "签约居民就诊人次数" decimal (10,
 0) DEFAULT NULL,
 "签约居民定点机构就诊人次数" decimal (10,
 0) DEFAULT NULL,
 "公共卫生服务经费金额" decimal (10,
 2) DEFAULT NULL,
 "财政投入金额" decimal (10,
 2) DEFAULT NULL,
 CONSTRAINT "家庭医生签约日结数据_机构(周期)"_"统计日期"_"机构代码"_PK PRIMARY KEY ("统计日期",
 "机构代码")
);


COMMENT ON TABLE "床位占用情况表" IS '对特定科室内床位使用情况的统计';
COMMENT ON COLUMN "床位占用情况表"."本日出院病人数" IS '该病区截止到采集当日的出院病人总数';
COMMENT ON COLUMN "床位占用情况表"."本日新转出病人数" IS '该病区截止到采集当日的新转出病人总数';
COMMENT ON COLUMN "床位占用情况表"."本日新转入病人数" IS '该病区截止到采集当日的新转入病人总数';
COMMENT ON COLUMN "床位占用情况表"."本日新入院病人数" IS '该病区截止到采集当日的新入院病人数';
COMMENT ON COLUMN "床位占用情况表"."本日仍留院病人数" IS '该病区截止到采集当日的留院病人总数';
COMMENT ON COLUMN "床位占用情况表"."出院者占用总床数" IS '该病区截止到采集当日的出院者占用床位总数，多科室共用病区不细分';
COMMENT ON COLUMN "床位占用情况表"."实际使用床位总数" IS '该病区截止到采集当日的实际使用床位总数，多科室共用病区不细分';
COMMENT ON COLUMN "床位占用情况表"."实际开放床位总数" IS '该病区截止到采集当日的实际开放床位总数，多科室共用病区不细分';
COMMENT ON COLUMN "床位占用情况表"."实际床位总数" IS '该病区截止到采集当日的实际床位总数，多科室共用病区不细分';
COMMENT ON COLUMN "床位占用情况表"."核定床位总数" IS '该病区截止到采集当日的编制床位总数，多科室共用病区不细分';
COMMENT ON COLUMN "床位占用情况表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "床位占用情况表"."病区(科室)名称" IS '病区(科室)在机构内编码体系中的名称。若医院以科室进行划分，则填写科室代码；若以病区划分的则填写病区代码。';
COMMENT ON COLUMN "床位占用情况表"."病区(科室)代码" IS '病区(科室)在机构内编码体系中的代码。若医院以科室进行划分，则填写科室代码；若以病区划分的则填写病区代码。';
COMMENT ON COLUMN "床位占用情况表"."采集时间" IS '数据采集当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "床位占用情况表"."采集日期" IS '数据采集当日的公元纪年日期';
COMMENT ON COLUMN "床位占用情况表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "床位占用情况表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "床位占用情况表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "床位占用情况表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "床位占用情况表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "床位占用情况表"."本日急诊留观实际使用床位数" IS '该病区截止到采集当日的急诊留观实际使用床位数';
COMMENT ON COLUMN "床位占用情况表"."本日ICU实际使用床位数" IS '该病区截止到采集当日的ICU实际使用床位数';
COMMENT ON COLUMN "床位占用情况表"."本日康复床位数" IS '该病区截止到采集当日的康复床位数';
COMMENT ON COLUMN "床位占用情况表"."本日急诊留观实际开放床位数" IS '该病区截止到采集当日的急诊留观实际开放床位数';
COMMENT ON COLUMN "床位占用情况表"."本日重症医学科实际开放床位数" IS '该病区截止到采集当日的重症医学科实际开放床位数';
COMMENT ON COLUMN "床位占用情况表"."本日负压床位数" IS '该病区截止到采集当日的负压床位数';
COMMENT ON COLUMN "床位占用情况表"."本日特需服务床位数" IS '该病区截止到采集当日的特需服务床位数';
COMMENT ON COLUMN "床位占用情况表"."本日空床数" IS '该病区截止到采集当日的空床总数';
COMMENT ON COLUMN "床位占用情况表"."本日加床数" IS '该病区截止到采集当日的加床总数';
COMMENT ON COLUMN "床位占用情况表"."本日死亡病人数" IS '该病区截止到采集当日的死亡病人总数';
CREATE TABLE IF NOT EXISTS "床位占用情况表" (
"本日出院病人数" decimal (10,
 0) DEFAULT NULL,
 "本日新转出病人数" decimal (10,
 0) DEFAULT NULL,
 "本日新转入病人数" decimal (10,
 0) DEFAULT NULL,
 "本日新入院病人数" decimal (10,
 0) DEFAULT NULL,
 "本日仍留院病人数" decimal (10,
 0) DEFAULT NULL,
 "出院者占用总床数" decimal (10,
 0) DEFAULT NULL,
 "实际使用床位总数" decimal (10,
 0) DEFAULT NULL,
 "实际开放床位总数" decimal (10,
 0) DEFAULT NULL,
 "实际床位总数" decimal (10,
 0) DEFAULT NULL,
 "核定床位总数" decimal (10,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "病区(科室)名称" varchar (100) DEFAULT NULL,
 "病区(科室)代码" varchar (20) NOT NULL,
 "采集时间" timestamp NOT NULL,
 "采集日期" date NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "本日急诊留观实际使用床位数" decimal (10,
 0) DEFAULT NULL,
 "本日ICU实际使用床位数" decimal (10,
 0) DEFAULT NULL,
 "本日康复床位数" decimal (15,
 0) DEFAULT NULL,
 "本日急诊留观实际开放床位数" decimal (10,
 0) DEFAULT NULL,
 "本日重症医学科实际开放床位数" decimal (10,
 0) DEFAULT NULL,
 "本日负压床位数" decimal (10,
 0) DEFAULT NULL,
 "本日特需服务床位数" decimal (10,
 0) DEFAULT NULL,
 "本日空床数" decimal (15,
 0) DEFAULT NULL,
 "本日加床数" decimal (15,
 0) DEFAULT NULL,
 "本日死亡病人数" decimal (10,
 0) DEFAULT NULL,
 CONSTRAINT "床位占用情况表"_"病区(科室)代码"_"采集时间"_"采集日期"_"医疗机构代码"_PK PRIMARY KEY ("病区(科室)代码",
 "采集时间",
 "采集日期",
 "医疗机构代码")
);


COMMENT ON TABLE "心室辅助记录" IS '心室辅助记录，包括心排指数、体外虚幻、植入设备类型、入径、并发症信息';
COMMENT ON COLUMN "心室辅助记录"."填表时间" IS '完成填表时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "心室辅助记录"."填表人姓名" IS '填表人在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "心室辅助记录"."填表人工号" IS '填表人在原始特定编码体系中的编号';
COMMENT ON COLUMN "心室辅助记录"."主治医生姓名" IS '主治医生在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "心室辅助记录"."主治医生工号" IS '主治医生在原始特定编码体系中的编号';
COMMENT ON COLUMN "心室辅助记录"."其他转归" IS '其他病情转归的详细描述';
COMMENT ON COLUMN "心室辅助记录"."死亡时间" IS '患者死亡当时的公元纪年和日期的完整描述';
COMMENT ON COLUMN "心室辅助记录"."移植时间" IS '移植完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "心室辅助记录"."撤机时间" IS '撤机完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "心室辅助记录"."出院时间" IS '办理完出院手续时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "心室辅助记录"."围手术期并发症" IS '围手术期并发症的分类代码';
COMMENT ON COLUMN "心室辅助记录"."术后并发症" IS '术后并发症的分类代码';
COMMENT ON COLUMN "心室辅助记录"."病房号" IS '患者入院时，所住病房对应的编号';
COMMENT ON COLUMN "心室辅助记录"."术中并发症" IS '术中并发症的分类代码';
COMMENT ON COLUMN "心室辅助记录"."医疗机构代码" IS '医疗机构在国家直报系统中的_12_位编码（如：_520000000001）';
COMMENT ON COLUMN "心室辅助记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "心室辅助记录"."心室辅助记录编号" IS '按照某一特定编码规则赋予心室辅助记录的唯一标识';
COMMENT ON COLUMN "心室辅助记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "心室辅助记录"."手术医生工号" IS '手术医生在原始特定编码体系中的编号';
COMMENT ON COLUMN "心室辅助记录"."手术名称" IS '手术名称的分类代码';
COMMENT ON COLUMN "心室辅助记录"."手术结束时间" IS '对患者结束手术操作时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "心室辅助记录"."手术开始时间" IS '以手术操作刀碰皮(切皮)时点计算，经自然腔道到达手术部位，没有皮肤切口的手术，开始时间以手术器械进入人体腔道的时点为准,对患者开始手术操作时的公元纪年日期和时间的完整描述。';
COMMENT ON COLUMN "心室辅助记录"."EF" IS '心脏射血分数，计量单位为%';
COMMENT ON COLUMN "心室辅助记录"."预计生存期达2年可能性小于50%标志" IS '标识是否预计生存期达2_年可能性小于50%的标志';
COMMENT ON COLUMN "心室辅助记录"."大剂量血管活性药物下循环功能难以维持标志" IS '标识是否大剂量血管活性药物下循环功能难以维持的标志';
COMMENT ON COLUMN "心室辅助记录"."经两个月正规药物治疗无治标志" IS '标识是否经两个月正规药物治疗无治的标志';
COMMENT ON COLUMN "心室辅助记录"."手术记录流水号" IS '按照某一特定编码规则赋予手术记录的唯一标识';
COMMENT ON COLUMN "心室辅助记录"."手术申请单号" IS '按照某一特定编码规则赋予手术申请单的唯一标识';
COMMENT ON COLUMN "心室辅助记录"."就诊科室代码" IS '按照机构内编码规则赋予所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "心室辅助记录"."就诊科室名称" IS '所在科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "心室辅助记录"."病区名称" IS '患者入院时，所住病区名称';
COMMENT ON COLUMN "心室辅助记录"."病床号" IS '患者入院时，所住床位对应的编号';
COMMENT ON COLUMN "心室辅助记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "心室辅助记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "心室辅助记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "心室辅助记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "心室辅助记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "心室辅助记录"."住院次数" IS '此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "心室辅助记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "心室辅助记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "心室辅助记录"."出生日期" IS '患者出生当日的公元纪年日期';
COMMENT ON COLUMN "心室辅助记录"."ABO血型代码" IS '受检者按照ABO血型系统决定的血型在特定编码体系中的代码';
COMMENT ON COLUMN "心室辅助记录"."Rh血型代码" IS '进行血型检查明确，或既往病历资料能够明确的患者Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "心室辅助记录"."术前主诊断代码" IS '按照机构内编码规则赋予术前主诊断的唯一标识，如有多个,用隔开';
COMMENT ON COLUMN "心室辅助记录"."术前主诊断名称" IS '术前主诊断在机构内编码规则中对应的名称，如有多个用,隔开';
COMMENT ON COLUMN "心室辅助记录"."术前次诊断代码" IS '按照机构内编码规则赋予术前次诊断的唯一标识，如有多个,用隔开';
COMMENT ON COLUMN "心室辅助记录"."术前次诊断名称" IS '术前次诊断在机构内编码规则中对应的名称，如有多个用,隔开';
COMMENT ON COLUMN "心室辅助记录"."心排指数" IS '心排指数的具体数值，计量单位为L/m^2';
COMMENT ON COLUMN "心室辅助记录"."最大耗氧量" IS '最大耗氧量的具体数值，计量单位为ml/kg·min';
COMMENT ON COLUMN "心室辅助记录"."混合静脉血氧饱和度" IS '混合静脉血氧饱和度的具体数值，计量单位为%';
COMMENT ON COLUMN "心室辅助记录"."6分钟步行距离" IS '6_分钟步行距离的具体数值，计量单位为m';
COMMENT ON COLUMN "心室辅助记录"."肺毛细血管楔压" IS '肺毛细血管楔压的具体数值，计量单位为mmHg';
COMMENT ON COLUMN "心室辅助记录"."NT-proBNP" IS 'NT-proBNP的具体数值，计量单位为ng/ml';
COMMENT ON COLUMN "心室辅助记录"."入径分类代码" IS '入径的分类在特定编码体系中的代码';
COMMENT ON COLUMN "心室辅助记录"."植入设备型号" IS '植入设备型号的具体描述';
COMMENT ON COLUMN "心室辅助记录"."心室辅助装置转机开始时间" IS '心室辅助装置转机开始时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "心室辅助记录"."心室辅助装置正常转机标志" IS '标识心室辅助装置是否正常转机的标志';
COMMENT ON COLUMN "心室辅助记录"."体外循环开始时间" IS '体外循环开始时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "心室辅助记录"."体外循环标志" IS '标识是否体外循环的标志';
COMMENT ON COLUMN "心室辅助记录"."手术医生姓名" IS '手术医生在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "心室辅助记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "心室辅助记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "心室辅助记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
CREATE TABLE IF NOT EXISTS "心室辅助记录" (
"填表时间" timestamp DEFAULT NULL,
 "填表人姓名" varchar (50) DEFAULT NULL,
 "填表人工号" varchar (20) DEFAULT NULL,
 "主治医生姓名" varchar (50) DEFAULT NULL,
 "主治医生工号" varchar (20) DEFAULT NULL,
 "其他转归" varchar (200) DEFAULT NULL,
 "死亡时间" timestamp DEFAULT NULL,
 "移植时间" timestamp DEFAULT NULL,
 "撤机时间" timestamp DEFAULT NULL,
 "出院时间" timestamp DEFAULT NULL,
 "围手术期并发症" varchar (50) DEFAULT NULL,
 "术后并发症" decimal (1,
 0) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "术中并发症" varchar (1) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "心室辅助记录编号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "手术医生工号" varchar (20) DEFAULT NULL,
 "手术名称" varchar (200) DEFAULT NULL,
 "手术结束时间" timestamp DEFAULT NULL,
 "手术开始时间" timestamp DEFAULT NULL,
 "EF" decimal (6,
 2) DEFAULT NULL,
 "预计生存期达2年可能性小于50%标志" varchar (1) DEFAULT NULL,
 "大剂量血管活性药物下循环功能难以维持标志" varchar (1) DEFAULT NULL,
 "经两个月正规药物治疗无治标志" varchar (1) DEFAULT NULL,
 "手术记录流水号" varchar (64) DEFAULT NULL,
 "手术申请单号" varchar (32) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "术前主诊断代码" varchar (64) DEFAULT NULL,
 "术前主诊断名称" varchar (200) DEFAULT NULL,
 "术前次诊断代码" varchar (50) DEFAULT NULL,
 "术前次诊断名称" varchar (200) DEFAULT NULL,
 "心排指数" decimal (6,
 2) DEFAULT NULL,
 "最大耗氧量" decimal (6,
 2) DEFAULT NULL,
 "混合静脉血氧饱和度" decimal (6,
 2) DEFAULT NULL,
 "6分钟步行距离" decimal (6,
 2) DEFAULT NULL,
 "肺毛细血管楔压" decimal (6,
 2) DEFAULT NULL,
 "NT-proBNP" decimal (6,
 2) DEFAULT NULL,
 "入径分类代码" varchar (1) DEFAULT NULL,
 "植入设备型号" varchar (50) DEFAULT NULL,
 "心室辅助装置转机开始时间" timestamp DEFAULT NULL,
 "心室辅助装置正常转机标志" varchar (1) DEFAULT NULL,
 "体外循环开始时间" timestamp DEFAULT NULL,
 "体外循环标志" varchar (1) DEFAULT NULL,
 "手术医生姓名" varchar (50) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 CONSTRAINT "心室辅助记录"_"医疗机构代码"_"心室辅助记录编号"_PK PRIMARY KEY ("医疗机构代码",
 "心室辅助记录编号")
);


COMMENT ON TABLE "患者基本信息表" IS '医疗机构内患者的个人基本属性信息';
COMMENT ON COLUMN "患者基本信息表"."建档机构名称" IS '建档医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "患者基本信息表"."建档医疗机构组织机构代码" IS '按照某一特定编码规则赋予建档医疗机构的唯一标识';
COMMENT ON COLUMN "患者基本信息表"."城乡居民健康档案编号" IS '按照某一特定编码规则赋予城乡居民健康档案的编号';
COMMENT ON COLUMN "患者基本信息表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "患者基本信息表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "患者基本信息表"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "患者基本信息表"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "患者基本信息表"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "患者基本信息表"."姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "患者基本信息表"."发卡地区" IS '指社保卡发卡地区';
COMMENT ON COLUMN "患者基本信息表"."社保卡号" IS '按照某一特定编码规则赋予患者社保卡的唯一标识';
COMMENT ON COLUMN "患者基本信息表"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "患者基本信息表"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "患者基本信息表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "患者基本信息表"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "患者基本信息表"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "患者基本信息表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "患者基本信息表"."出生次序(胎次)" IS '新生儿在多胞胎中的排列顺序';
COMMENT ON COLUMN "患者基本信息表"."多胞胎标志" IS '标识新生儿是否属于多胞胎之一';
COMMENT ON COLUMN "患者基本信息表"."母亲身份证件号码" IS '新生儿的母亲的身份证件号码';
COMMENT ON COLUMN "患者基本信息表"."母亲姓名" IS '新生儿的母亲姓名';
COMMENT ON COLUMN "患者基本信息表"."新生儿标志" IS '标识患者是否为新生儿的标志';
COMMENT ON COLUMN "患者基本信息表"."联系人电话号码" IS '联系人的电话号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "患者基本信息表"."联系人邮编" IS '联系人地址中所在行政区的邮政编码';
COMMENT ON COLUMN "患者基本信息表"."联系人地址" IS '联系人当前常驻地址或工作单位地址的详细描述';
COMMENT ON COLUMN "患者基本信息表"."联系人关系名称" IS '联系人与患者之间的关系类别(如配偶、子女、父母等)在特定编码体系中的名称';
COMMENT ON COLUMN "患者基本信息表"."联系人关系代码" IS '联系人与患者之间的关系类别(如配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "患者基本信息表"."联系人姓名" IS '联系人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "患者基本信息表"."户口地址邮编" IS '户口地址中所在行政区的邮政编码';
COMMENT ON COLUMN "患者基本信息表"."户口地址" IS '户口地址的详细描述';
COMMENT ON COLUMN "患者基本信息表"."居住地址" IS '现居住地址的详细描述';
COMMENT ON COLUMN "患者基本信息表"."工作单位电话号码" IS '工作单位地址的电话号码的描述';
COMMENT ON COLUMN "患者基本信息表"."工作单位地址" IS '工作单位地址的详细描述';
COMMENT ON COLUMN "患者基本信息表"."工作单位名称" IS '个体工作单位的组织机构名称';
COMMENT ON COLUMN "患者基本信息表"."工作单位邮编" IS '所在的工作单位地址的邮政编码';
COMMENT ON COLUMN "患者基本信息表"."出生地-区划代码" IS '出生地址的6位区县行政区划码+省统计局发布的3位乡镇编码和3位村编码';
COMMENT ON COLUMN "患者基本信息表"."手机号码" IS '居民本人电话号码，含区号和分机号';
COMMENT ON COLUMN "患者基本信息表"."电话号码" IS '本人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "患者基本信息表"."职业类别代码" IS '本人从事职业所属类别(如国家公务员、专业技术人员、职员、工人等)在特定编码体系中的代码';
COMMENT ON COLUMN "患者基本信息表"."国籍名称" IS '所属国籍在特定编码体系中的名称';
COMMENT ON COLUMN "患者基本信息表"."国籍代码" IS '所属国籍在特定编码体系中的代码';
COMMENT ON COLUMN "患者基本信息表"."民族名称" IS '所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "患者基本信息表"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "患者基本信息表"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "患者基本信息表"."学历代码" IS '个体受教育最高程度的类别(如研究生教育、大学本科、专科教育等)在特定编码体系中的代码';
COMMENT ON COLUMN "患者基本信息表"."婚姻状况名称" IS '当前婚姻状况在特定编码体系中的名称，如已婚、未婚、初婚等';
COMMENT ON COLUMN "患者基本信息表"."婚姻状况代码" IS '当前婚姻状况(已婚、未婚、初婚等)在特定编码体系中的代码';
COMMENT ON COLUMN "患者基本信息表"."保险类型代码" IS '个体参加的医疗保险类别(如城镇职工基本医疗保险、城镇居民基本医疗保险等)在特定编码体系中的代码';
COMMENT ON COLUMN "患者基本信息表"."常住户籍类型代码" IS '患者类型(如本市、外地、境外(港澳台)、外国、未知等)在特定编码体系中的代码';
COMMENT ON COLUMN "患者基本信息表"."性别名称" IS '个体生理性别在特定编码体系中的名称';
COMMENT ON COLUMN "患者基本信息表"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "患者基本信息表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "患者基本信息表"."更新人身份证号" IS '更新人居民身份证上的唯一法定标识符';
COMMENT ON COLUMN "患者基本信息表"."更新人姓名" IS '更新人姓在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "患者基本信息表"."业务数据更新时间" IS '业务信息最后更新的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "患者基本信息表"."业务数据生成时间" IS '业务操作获取该患者信息的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "患者基本信息表"."建档时间" IS '建档完成时公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "患者基本信息表"."建档医生姓名" IS '首次为患者建立电子病历的人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "患者基本信息表"."建档医生工号" IS '首次为患者建立电子病历的人员的工号';
COMMENT ON COLUMN "患者基本信息表"."建档机构联系电话" IS '建档机构的联系电话，包括区号';
CREATE TABLE IF NOT EXISTS "患者基本信息表" (
"建档机构名称" varchar (70) DEFAULT NULL,
 "建档医疗机构组织机构代码" varchar (22) DEFAULT NULL,
 "城乡居民健康档案编号" varchar (50) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型名称" varchar (20) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "发卡地区" varchar (6) DEFAULT NULL,
 "社保卡号" varchar (20) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "患者唯一标识号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "出生次序(胎次)" varchar (2) DEFAULT NULL,
 "多胞胎标志" varchar (1) DEFAULT NULL,
 "母亲身份证件号码" varchar (50) DEFAULT NULL,
 "母亲姓名" varchar (50) DEFAULT NULL,
 "新生儿标志" varchar (1) DEFAULT NULL,
 "联系人电话号码" varchar (20) DEFAULT NULL,
 "联系人邮编" varchar (6) DEFAULT NULL,
 "联系人地址" varchar (20) DEFAULT NULL,
 "联系人关系名称" varchar (100) DEFAULT NULL,
 "联系人关系代码" varchar (4) DEFAULT NULL,
 "联系人姓名" varchar (50) DEFAULT NULL,
 "户口地址邮编" varchar (6) DEFAULT NULL,
 "户口地址" varchar (128) DEFAULT NULL,
 "居住地址" varchar (200) DEFAULT NULL,
 "工作单位电话号码" varchar (32) DEFAULT NULL,
 "工作单位地址" varchar (128) DEFAULT NULL,
 "工作单位名称" varchar (70) DEFAULT NULL,
 "工作单位邮编" varchar (6) DEFAULT NULL,
 "出生地-区划代码" varchar (32) DEFAULT NULL,
 "手机号码" varchar (20) DEFAULT NULL,
 "电话号码" varchar (32) DEFAULT NULL,
 "职业类别代码" varchar (4) DEFAULT NULL,
 "国籍名称" varchar (20) DEFAULT NULL,
 "国籍代码" varchar (10) DEFAULT NULL,
 "民族名称" varchar (20) DEFAULT NULL,
 "民族代码" varchar (2) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "学历代码" varchar (5) DEFAULT NULL,
 "婚姻状况名称" varchar (10) DEFAULT NULL,
 "婚姻状况代码" varchar (2) DEFAULT NULL,
 "保险类型代码" varchar (2) DEFAULT NULL,
 "常住户籍类型代码" varchar (1) DEFAULT NULL,
 "性别名称" varchar (10) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "更新人身份证号" varchar (20) DEFAULT NULL,
 "更新人姓名" varchar (50) DEFAULT NULL,
 "业务数据更新时间" timestamp DEFAULT NULL,
 "业务数据生成时间" timestamp DEFAULT NULL,
 "建档时间" timestamp DEFAULT NULL,
 "建档医生姓名" varchar (50) DEFAULT NULL,
 "建档医生工号" varchar (20) DEFAULT NULL,
 "建档机构联系电话" varchar (20) DEFAULT NULL,
 CONSTRAINT "患者基本信息表"_"患者唯一标识号"_"医疗机构代码"_PK PRIMARY KEY ("患者唯一标识号",
 "医疗机构代码")
);


COMMENT ON TABLE "手术目录" IS '医疗机构内手术名称，包含院内手术与平台手术映射关系';
COMMENT ON COLUMN "手术目录"."手术名称" IS '医疗机构内手术名称';
COMMENT ON COLUMN "手术目录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "手术目录"."手术代码" IS '医疗机构内手术代码';
COMMENT ON COLUMN "手术目录"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "手术目录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "手术目录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术目录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "手术目录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "手术目录"."记录状态" IS '标识记录是否正常使用的标志，其中：0：正常；1：停用';
COMMENT ON COLUMN "手术目录"."平台手术名称" IS '平台中心手术名称';
COMMENT ON COLUMN "手术目录"."平台手术代码" IS '手术及操作在特定编码体系中的唯一标识';
CREATE TABLE IF NOT EXISTS "手术目录" (
"手术名称" varchar (200) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "手术代码" varchar (20) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "记录状态" varchar (1) DEFAULT NULL,
 "平台手术名称" varchar (128) DEFAULT NULL,
 "平台手术代码" varchar (20) DEFAULT NULL,
 CONSTRAINT "手术目录"_"手术代码"_"医疗机构代码"_PK PRIMARY KEY ("手术代码",
 "医疗机构代码")
);


COMMENT ON TABLE "抢救记录表" IS '病危患者抢救时的记录，包括诊断、体征、抢救时间和结果信息';
COMMENT ON COLUMN "抢救记录表"."就诊流水号" IS '按照某一特定编码规则赋予就诊事件的唯一标识，同门急诊/住院关联';
COMMENT ON COLUMN "抢救记录表"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "抢救记录表"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "抢救记录表"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "抢救记录表"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "抢救记录表"."姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "抢救记录表"."性别名称" IS '一般指患者医学生理性别，指男性或女性';
COMMENT ON COLUMN "抢救记录表"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "抢救记录表"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "抢救记录表"."诊断疾病代码" IS '平台中心诊断代码，患者所患的疾病诊断特定编码体系中的编码';
COMMENT ON COLUMN "抢救记录表"."诊断分类代码" IS '诊断分类(如西医诊断、中医症候、中医疾病等)在特定编码体系中的代码';
COMMENT ON COLUMN "抢救记录表"."病情变化情况" IS '对患者病情变化的详细描述';
COMMENT ON COLUMN "抢救记录表"."抢救经过" IS '进行抢救过程中的详细描述';
COMMENT ON COLUMN "抢救记录表"."抢救措施" IS '进行抢救过程中采取的措施';
COMMENT ON COLUMN "抢救记录表"."生命体征" IS '对受检者生命体征的详细描述';
COMMENT ON COLUMN "抢救记录表"."抢救结束时间" IS '实施抢救的结束时间的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "抢救记录表"."抢救开始时间" IS '实施抢救的开始时间的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "抢救记录表"."抢救时间" IS '实施抢救时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "抢救记录表"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "抢救记录表"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "抢救记录表"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "抢救记录表"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "抢救记录表"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "抢救记录表"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "抢救记录表"."诊断疾病名称" IS '平台中心诊断名称，患者所患疾病的西医诊断名称';
COMMENT ON COLUMN "抢救记录表"."检查/检验项目名称" IS '患者检查/检验项目的正式名称';
COMMENT ON COLUMN "抢救记录表"."检查/检验结果" IS '患者检查/检验结果的描述';
COMMENT ON COLUMN "抢救记录表"."检查/检验定量结果" IS '患者检查/检验结果的测量值(定量)';
COMMENT ON COLUMN "抢救记录表"."检查/检验结果单位" IS '患者定量检查/检验测量值的计量单位';
COMMENT ON COLUMN "抢救记录表"."手术及操作代码" IS '患者住院期间实施的手术及操作在特定编码体系中的编码';
COMMENT ON COLUMN "抢救记录表"."手术及操作名称" IS '按照手术操作分类代码国家临床版 2.0对应的手术名称';
COMMENT ON COLUMN "抢救记录表"."手术及操作部位名称" IS '实施手术/操作的人体目标部位名称';
COMMENT ON COLUMN "抢救记录表"."介入物名称" IS '实施手术操作时使用/放置的材料/药物的名称';
COMMENT ON COLUMN "抢救记录表"."操作方法描述" IS '手术/操作方法的详细描述';
COMMENT ON COLUMN "抢救记录表"."操作次数" IS '实施操作的次数';
COMMENT ON COLUMN "抢救记录表"."抢救成功标志" IS '标识本次抢救是否成功';
COMMENT ON COLUMN "抢救记录表"."死亡时间" IS '患者死亡当时的公元纪年和日期的完整描述';
COMMENT ON COLUMN "抢救记录表"."注意事项" IS '对可能出现问题及采取相应措施的描述';
COMMENT ON COLUMN "抢救记录表"."抢救人员名单" IS '参加抢救医务人员的姓名列表，以”，“分隔';
COMMENT ON COLUMN "抢救记录表"."抢救科室代码" IS '抢救科室在机构内编码体系中的代码';
COMMENT ON COLUMN "抢救记录表"."抢救科室名称" IS '抢救科室在机构编码体系中的名称';
COMMENT ON COLUMN "抢救记录表"."医师工号" IS '医师在特定编码体系中的编号';
COMMENT ON COLUMN "抢救记录表"."医师姓名" IS '医师在公安户籍管理部门正式登记注册的姓氏和名称。本处指记录医师姓名';
COMMENT ON COLUMN "抢救记录表"."签名时间" IS '医师姓名完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "抢救记录表"."专业技术职务类别代码" IS '医护人员专业技术职务分类（如正高、副高、中级、助理等）在特定编码体系中的代码';
COMMENT ON COLUMN "抢救记录表"."专业技术职务类别名称" IS '医护人员专业技术职务分类（如正高、副高、中级、助理等）';
COMMENT ON COLUMN "抢救记录表"."记录人身份证号" IS '记录人居民身份证上的唯一法定标识符';
COMMENT ON COLUMN "抢救记录表"."记录人姓名" IS '记录人在公安户籍管理部门正式登记注册的姓氏和名称。本处指记录医师姓名';
COMMENT ON COLUMN "抢救记录表"."记录时间" IS '记录时间的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "抢救记录表"."文本内容" IS '存入大文本的内容，最大不超过64K';
COMMENT ON COLUMN "抢救记录表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "抢救记录表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "抢救记录表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "抢救记录表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "抢救记录表"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "抢救记录表"."住院次数" IS '办理完整住院治疗手续的次数';
COMMENT ON COLUMN "抢救记录表"."抢救流水号" IS '按照某一特性编码规则赋予本次抢救记录的唯一标识';
COMMENT ON COLUMN "抢救记录表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "抢救记录表"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
CREATE TABLE IF NOT EXISTS "抢救记录表" (
"就诊流水号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "诊断疾病代码" varchar (20) DEFAULT NULL,
 "诊断分类代码" varchar (1) DEFAULT NULL,
 "病情变化情况" varchar (1000) DEFAULT NULL,
 "抢救经过" varchar (800) DEFAULT NULL,
 "抢救措施" varchar (1000) DEFAULT NULL,
 "生命体征" varchar (800) DEFAULT NULL,
 "抢救结束时间" timestamp DEFAULT NULL,
 "抢救开始时间" timestamp DEFAULT NULL,
 "抢救时间" timestamp DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "诊断疾病名称" varchar (200) DEFAULT NULL,
 "检查/检验项目名称" varchar (80) DEFAULT NULL,
 "检查/检验结果" varchar (1000) DEFAULT NULL,
 "检查/检验定量结果" decimal (18,
 4) DEFAULT NULL,
 "检查/检验结果单位" varchar (10) DEFAULT NULL,
 "手术及操作代码" varchar (20) DEFAULT NULL,
 "手术及操作名称" varchar (80) DEFAULT NULL,
 "手术及操作部位名称" varchar (50) DEFAULT NULL,
 "介入物名称" varchar (100) DEFAULT NULL,
 "操作方法描述" text,
 "操作次数" decimal (3,
 0) DEFAULT NULL,
 "抢救成功标志" varchar (1) DEFAULT NULL,
 "死亡时间" timestamp DEFAULT NULL,
 "注意事项" text,
 "抢救人员名单" varchar (200) DEFAULT NULL,
 "抢救科室代码" varchar (20) DEFAULT NULL,
 "抢救科室名称" varchar (100) DEFAULT NULL,
 "医师工号" varchar (30) DEFAULT NULL,
 "医师姓名" varchar (50) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "专业技术职务类别代码" varchar (50) DEFAULT NULL,
 "专业技术职务类别名称" varchar (50) DEFAULT NULL,
 "记录人身份证号" varchar (18) DEFAULT NULL,
 "记录人姓名" varchar (50) DEFAULT NULL,
 "记录时间" timestamp DEFAULT NULL,
 "文本内容" text,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "抢救流水号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 CONSTRAINT "抢救记录表"_"医疗机构代码"_"抢救流水号"_PK PRIMARY KEY ("医疗机构代码",
 "抢救流水号")
);


COMMENT ON TABLE "接诊记录" IS '患者转院时接诊医院关于患者主观资料、客观资料以及处置计划的记录';
COMMENT ON COLUMN "接诊记录"."接诊单号" IS '按照某一特定编码规则赋予接诊单的顺序号';
COMMENT ON COLUMN "接诊记录"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "接诊记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "接诊记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "接诊记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "接诊记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "接诊记录"."接诊时间" IS '患者转入到现管理单位的公元纪年日期和时间完整描述';
COMMENT ON COLUMN "接诊记录"."接诊医生姓名" IS '转入科室经治医师本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "接诊记录"."接诊医生工号" IS '转入科室经治医师的工号';
COMMENT ON COLUMN "接诊记录"."处置计划" IS '对患者情况进行综合评估的基础上，为其制定的处置计划的详细描述';
COMMENT ON COLUMN "接诊记录"."评估" IS '根据患者疾病临床表现、实验室检查结果等作出的健康问题评估结果的详细描述';
COMMENT ON COLUMN "接诊记录"."客观资料" IS '患者客观检查检验情况的描述';
COMMENT ON COLUMN "接诊记录"."主观资料" IS '患者主观情况的描述';
COMMENT ON COLUMN "接诊记录"."身份证号" IS '患者居民身份证上的唯一法定标识符';
COMMENT ON COLUMN "接诊记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
CREATE TABLE IF NOT EXISTS "接诊记录" (
"接诊单号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "接诊时间" timestamp DEFAULT NULL,
 "接诊医生姓名" varchar (50) DEFAULT NULL,
 "接诊医生工号" varchar (20) DEFAULT NULL,
 "处置计划" text,
 "评估" text,
 "客观资料" text,
 "主观资料" text,
 "身份证号" varchar (18) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 CONSTRAINT "接诊记录"_"接诊单号"_"医疗机构代码"_PK PRIMARY KEY ("接诊单号",
 "医疗机构代码")
);


COMMENT ON TABLE "教学情况基本信息表" IS '医疗机构教学情况统计，包括在院实习和学习的总人数、博士研究生人数、硕士研究生人数等';
COMMENT ON COLUMN "教学情况基本信息表"."其中，大专生人数" IS '大专生人数';
COMMENT ON COLUMN "教学情况基本信息表"."账套编码" IS '按照某一特定规则赋予账套的唯一标识';
COMMENT ON COLUMN "教学情况基本信息表"."统计年份" IS '统计年份';
COMMENT ON COLUMN "教学情况基本信息表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "教学情况基本信息表"."其中，本科生人数" IS '本科生人数';
COMMENT ON COLUMN "教学情况基本信息表"."其中，硕士研究生人数" IS '硕士研究生人数';
COMMENT ON COLUMN "教学情况基本信息表"."其中，博士研究生人数" IS '博士研究生人数';
COMMENT ON COLUMN "教学情况基本信息表"."在院实习和学习的总人数" IS '在院实习和学习的总人数';
COMMENT ON COLUMN "教学情况基本信息表"."统计月份" IS '统计月份';
COMMENT ON COLUMN "教学情况基本信息表"."教学情况基本信息表编号" IS '按照某一特定规则赋予教学情况基本信息表的唯一标识';
COMMENT ON COLUMN "教学情况基本信息表"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "教学情况基本信息表"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "教学情况基本信息表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "教学情况基本信息表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "教学情况基本信息表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "教学情况基本信息表"."承担住院医师培养人数" IS '承担住院医师培养人数';
COMMENT ON COLUMN "教学情况基本信息表"."各类进修总人数" IS '各类进修总人数';
COMMENT ON COLUMN "教学情况基本信息表"."其中，实习生人数" IS '实习生人数';
CREATE TABLE IF NOT EXISTS "教学情况基本信息表" (
"其中，大专生人数" decimal (10,
 0) DEFAULT NULL,
 "账套编码" varchar (32) DEFAULT NULL,
 "统计年份" varchar (4) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "其中，本科生人数" decimal (10,
 0) DEFAULT NULL,
 "其中，硕士研究生人数" decimal (10,
 0) DEFAULT NULL,
 "其中，博士研究生人数" decimal (10,
 0) DEFAULT NULL,
 "在院实习和学习的总人数" decimal (10,
 0) DEFAULT NULL,
 "统计月份" varchar (2) DEFAULT NULL,
 "教学情况基本信息表编号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "承担住院医师培养人数" decimal (10,
 0) DEFAULT NULL,
 "各类进修总人数" decimal (10,
 0) DEFAULT NULL,
 "其中，实习生人数" decimal (10,
 0) DEFAULT NULL,
 CONSTRAINT "教学情况基本信息表"_"教学情况基本信息表编号"_"医疗机构代码"_PK PRIMARY KEY ("教学情况基本信息表编号",
 "医疗机构代码")
);


COMMENT ON TABLE "数据对账表" IS '数据对账信息，如对账时间段内的门诊就诊人次、门诊结算总额、住院入院人次、住院结算人次、住院结算总额等';
COMMENT ON COLUMN "数据对账表"."住院结算人次" IS '该时间对内住院结算人次数';
COMMENT ON COLUMN "数据对账表"."住院结算总额" IS '该时间段内住院结算总额，计量单位元';
COMMENT ON COLUMN "数据对账表"."表名" IS '机构向区域平台数据中心填报的数据表中文名';
COMMENT ON COLUMN "数据对账表"."发送数量" IS '机构向区域平台数据中心填报的数据表对应的数据量';
COMMENT ON COLUMN "数据对账表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "数据对账表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "数据对账表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "数据对账表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "数据对账表"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "数据对账表"."对账流水号" IS '按照某一特定编码规则赋予数据对账记录的唯一标识号';
COMMENT ON COLUMN "数据对账表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "数据对账表"."对账开始日期" IS '对账开始时的公元纪年日期的完整描述';
COMMENT ON COLUMN "数据对账表"."对账结束日期" IS '对账结束时的公元纪年日期的完整描述';
COMMENT ON COLUMN "数据对账表"."门诊就诊人次" IS '该时间段内门诊就诊人次总和，包括急诊就诊';
COMMENT ON COLUMN "数据对账表"."门诊结算总额" IS '该时间段内门诊结算总额，计量单位元';
COMMENT ON COLUMN "数据对账表"."住院入院人次" IS '该时间段内住院入院人次数';
CREATE TABLE IF NOT EXISTS "数据对账表" (
"住院结算人次" decimal (10,
 0) DEFAULT NULL,
 "住院结算总额" decimal (16,
 0) DEFAULT NULL,
 "表名" varchar (50) DEFAULT NULL,
 "发送数量" decimal (10,
 0) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "对账流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "对账开始日期" date DEFAULT NULL,
 "对账结束日期" date DEFAULT NULL,
 "门诊就诊人次" decimal (10,
 0) DEFAULT NULL,
 "门诊结算总额" decimal (16,
 0) DEFAULT NULL,
 "住院入院人次" decimal (10,
 0) DEFAULT NULL,
 CONSTRAINT "数据对账表"_"医疗机构代码"_"对账流水号"_PK PRIMARY KEY ("医疗机构代码",
 "对账流水号")
);


COMMENT ON TABLE "文件索引表" IS '患者就诊相关文件信息、包括文档类型、文档明细类型、文档格式、数据来源等';
COMMENT ON COLUMN "文件索引表"."门(急)诊号" IS '按照某一特定编码规则赋予门(急)诊就诊对象的顺序号';
COMMENT ON COLUMN "文件索引表"."就诊时间" IS '患者就诊结束时的公元纪年日期和时间的完整描述，对于住院业务，填写入院时间';
COMMENT ON COLUMN "文件索引表"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "文件索引表"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "文件索引表"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "文件索引表"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "文件索引表"."文档名称" IS '文档名称，不含路径';
COMMENT ON COLUMN "文件索引表"."文档标识号" IS '按照某一特定编码规则赋予文档的唯一标识，例如：检验报告，WDBSH传JYJLB(检验记录表)中的JYJLLSH(检验记录流水号)';
COMMENT ON COLUMN "文件索引表"."文档类别代码" IS '文档类别(如病历、体检报告、CDA、护理病历等)在特定编码体系中的代码';
COMMENT ON COLUMN "文件索引表"."文档明细类别代码" IS '文档明细类别(如病历摘要、患者基本信息、住院病案首页、常规医疗同意书等)在特定编码体系中的代码';
COMMENT ON COLUMN "文件索引表"."文档格式类别代码" IS '文档格式(如PNG、JPG、XML、TXT等)在特定编码体系中的代码';
COMMENT ON COLUMN "文件索引表"."文档内容" IS '文档存储内容，支持两种上传方式，当使用OSS对象存储时，该字段保存文件对象的key；使用文件存储时，该字段保存文件的Base64字符串(<1M)\r\n修改标志为撤销时可为空';
COMMENT ON COLUMN "文件索引表"."数据来源" IS '数据的来源系统描述';
COMMENT ON COLUMN "文件索引表"."创建时间" IS '医师创建时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "文件索引表"."创建医生姓名" IS '医师最后更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "文件索引表"."最后更新时间" IS '按照某一特定编码规则赋予最后更新医师的唯一标识';
COMMENT ON COLUMN "文件索引表"."最后更新医生姓名" IS '最后更新医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "文件索引表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "文件索引表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "文件索引表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "文件索引表"."医疗机构代码" IS '按照某一特定编码规则赋予医疗机构的唯一标识';
COMMENT ON COLUMN "文件索引表"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "文件索引表"."就诊流水号" IS '按照某一特定编码规则赋予特定业务事件的唯一标识，如门急诊就诊流水号、住院就诊流水号。对门诊类型，一般可采用挂号时HIS产生的就诊流水号；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "文件索引表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "文件索引表"."医保机构代码" IS '医保11位机构代码';
COMMENT ON COLUMN "文件索引表"."文档产生科室代码" IS '按照机构内编码规则赋予文档产生科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "文件索引表"."文档产生科室名称" IS '文档产生科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "文件索引表"."就诊类别" IS '患者就诊类别在标准体系中的代码';
COMMENT ON COLUMN "文件索引表"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
CREATE TABLE IF NOT EXISTS "文件索引表" (
"门(急)诊号" varchar (32) DEFAULT NULL,
 "就诊时间" timestamp DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "文档名称" varchar (256) DEFAULT NULL,
 "文档标识号" varchar (64) NOT NULL,
 "文档类别代码" varchar (2) DEFAULT NULL,
 "文档明细类别代码" varchar (10) DEFAULT NULL,
 "文档格式类别代码" varchar (2) DEFAULT NULL,
 "文档内容" text,
 "数据来源" varchar (50) DEFAULT NULL,
 "创建时间" timestamp DEFAULT NULL,
 "创建医生姓名" varchar (50) DEFAULT NULL,
 "最后更新时间" timestamp DEFAULT NULL,
 "最后更新医生姓名" varchar (50) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "就诊流水号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "医保机构代码" varchar (11) DEFAULT NULL,
 "文档产生科室代码" varchar (20) DEFAULT NULL,
 "文档产生科室名称" varchar (100) DEFAULT NULL,
 "就诊类别" varchar (2) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 CONSTRAINT "文件索引表"_"文档标识号"_"医疗机构代码"_"就诊流水号"_PK PRIMARY KEY ("文档标识号",
 "医疗机构代码",
 "就诊流水号")
);


COMMENT ON TABLE "新冠核酸或抗原阳性监管统计表" IS '按日统计医院新冠核酸或抗原阳性数，如新冠核酸或抗原阳性的总在院数、儿科在院数等';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."其他专科ICU在院数" IS '截止统计当日的新冠核酸或抗原阳性患者其他专科ICU患者在院人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."综合ICU在院数" IS '截止统计当日的新冠核酸或抗原阳性患者中综合ICU患者在院人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."PICU在院数" IS '截止统计当日的新冠核酸或抗原阳性患者中PICU患者在院人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."呼吸专科ICU在院数" IS '截止统计当日的新冠核酸或抗原阳性患者中呼吸专科ICU患者总在院人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."ICU在院数" IS '截止统计当日的新冠核酸或抗原阳性患者中ICU患者总在院人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."日间手术量" IS '截止统计当日的新冠核酸或抗原阳性患者日间手术量';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."儿科在院数" IS '截止统计当日的新冠核酸或抗原阳性儿科患者总在院人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."总在院数" IS '截止统计当日的新冠核酸或抗原阳性患者总在院人数';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."数据日期" IS '数据统计当日的公元纪年日期';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "新冠核酸或抗原阳性监管统计表"."NICU在院数" IS '截止统计当日的新冠核酸或抗原阳性患者NICU患者在院人数';
CREATE TABLE IF NOT EXISTS "新冠核酸或抗原阳性监管统计表" (
"其他专科ICU在院数" decimal (4,
 0) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "综合ICU在院数" decimal (4,
 0) DEFAULT NULL,
 "PICU在院数" decimal (4,
 0) DEFAULT NULL,
 "呼吸专科ICU在院数" decimal (4,
 0) DEFAULT NULL,
 "ICU在院数" decimal (4,
 0) DEFAULT NULL,
 "日间手术量" decimal (4,
 0) DEFAULT NULL,
 "儿科在院数" decimal (4,
 0) DEFAULT NULL,
 "总在院数" decimal (4,
 0) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "数据日期" date NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "NICU在院数" decimal (4,
 0) DEFAULT NULL,
 CONSTRAINT "新冠核酸或抗原阳性监管统计表"_"数据日期"_"医疗机构代码"_PK PRIMARY KEY ("数据日期",
 "医疗机构代码")
);


COMMENT ON TABLE "术前小结" IS '手术前患者诊断、检查结果以及拟实施手术和注意事项的记录';
COMMENT ON COLUMN "术前小结"."拟实施手术部位名称" IS '拟实施手术的人体目标部位的名称，如双侧鼻孔、臀部、左臂、右眼等';
COMMENT ON COLUMN "术前小结"."拟实施手术及操作名称" IS '拟实施的手术及操作在特定编码体系中的名称';
COMMENT ON COLUMN "术前小结"."拟实施手术及操作代码" IS '按照平台特定编码规则赋予拟实施的手术及操作的唯一标识';
COMMENT ON COLUMN "术前小结"."会诊意见" IS '由会诊医师填写患者会诊时的主要处置、指导意见的详细描述';
COMMENT ON COLUMN "术前小结"."手术指征" IS '患者具备的、适宜实施手术的主要症状和体征描述';
COMMENT ON COLUMN "术前小结"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "术前小结"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "术前小结"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "术前小结"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "术前小结"."住院次数" IS '办理完整住院治疗手续的次数';
COMMENT ON COLUMN "术前小结"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "术前小结"."性别名称" IS '一般指患者医学生理性别，指男性或女性';
COMMENT ON COLUMN "术前小结"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "术前小结"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "术前小结"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "术前小结"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "术前小结"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "术前小结"."小结时间" IS '记录小结完成的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "术前小结"."病历摘要" IS '对患者病情摘要的详细描述';
COMMENT ON COLUMN "术前小结"."电子申请单编号" IS '按照某一特定编码规则赋予电子申请单的顺序号';
COMMENT ON COLUMN "术前小结"."术前诊断代码" IS '按照平台编码规则赋予术前诊断的唯一标识';
COMMENT ON COLUMN "术前小结"."术前诊断名称" IS '术前已经诊疗确认的疾病名称';
COMMENT ON COLUMN "术前小结"."诊断依据" IS '对疾病诊断依据的详细描述';
COMMENT ON COLUMN "术前小结"."过敏史标志" IS '标识患者有无过敏经历的标志';
COMMENT ON COLUMN "术前小结"."过敏史" IS '患者既往发生过敏情况的详细描述';
COMMENT ON COLUMN "术前小结"."手术要点" IS '手术要点的详细描述';
COMMENT ON COLUMN "术前小结"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "术前小结"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "术前小结"."术前小结流水号" IS '按照某一特性编码规则赋予术前小结的唯一标识';
COMMENT ON COLUMN "术前小结"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "术前小结"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "术前小结"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "术前小结"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "术前小结"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "术前小结"."科室代码" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "术前小结"."辅助检查结果" IS '受检者辅助检查结果的详细描述';
COMMENT ON COLUMN "术前小结"."手术适应证" IS '手术适应证的详细描述';
COMMENT ON COLUMN "术前小结"."手术禁忌症" IS '拟实施手术的禁忌症的详细描述';
COMMENT ON COLUMN "术前小结"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "术前小结"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "术前小结"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "术前小结"."文本内容" IS '存入大文本的内容，最大不超过64K';
COMMENT ON COLUMN "术前小结"."签名时间" IS '医师姓名完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "术前小结"."医师姓名" IS '医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "术前小结"."医师工号" IS '负责住院小结的医师的工号';
COMMENT ON COLUMN "术前小结"."手术者姓名" IS '实施手术的主要执行人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "术前小结"."手术者工号" IS '实施手术的主要执行人员在原始特定编码体系中的编号';
COMMENT ON COLUMN "术前小结"."术前准备" IS '手术前准备工作的详细描述';
COMMENT ON COLUMN "术前小结"."科室名称" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "术前小结"."注意事项" IS '对可能出现问题及采取相应措施的描述';
COMMENT ON COLUMN "术前小结"."拟实施麻醉方法名称" IS '为患者进行手 术、操作时拟使用的麻醉方法';
COMMENT ON COLUMN "术前小结"."拟实施麻醉方法代码" IS '拟为患者进行手术、操作时使用的麻醉方法在特定编码体系中的代码';
COMMENT ON COLUMN "术前小结"."拟实施手术及操作时间" IS '拟对患者开始手术操作时的公元纪年日期和时间的完整描述';
CREATE TABLE IF NOT EXISTS "术前小结" (
"拟实施手术部位名称" varchar (50) DEFAULT NULL,
 "拟实施手术及操作名称" varchar (80) DEFAULT NULL,
 "拟实施手术及操作代码" varchar (50) DEFAULT NULL,
 "会诊意见" text,
 "手术指征" varchar (500) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "小结时间" timestamp DEFAULT NULL,
 "病历摘要" varchar (200) DEFAULT NULL,
 "电子申请单编号" varchar (100) DEFAULT NULL,
 "术前诊断代码" varchar (64) DEFAULT NULL,
 "术前诊断名称" varchar (100) DEFAULT NULL,
 "诊断依据" text,
 "过敏史标志" varchar (1) DEFAULT NULL,
 "过敏史" text,
 "手术要点" varchar (200) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "术前小结流水号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "辅助检查结果" varchar (1000) DEFAULT NULL,
 "手术适应证" varchar (100) DEFAULT NULL,
 "手术禁忌症" varchar (100) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "文本内容" text,
 "签名时间" timestamp DEFAULT NULL,
 "医师姓名" varchar (50) DEFAULT NULL,
 "医师工号" varchar (20) DEFAULT NULL,
 "手术者姓名" varchar (50) DEFAULT NULL,
 "手术者工号" varchar (20) DEFAULT NULL,
 "术前准备" text,
 "科室名称" varchar (100) DEFAULT NULL,
 "注意事项" text,
 "拟实施麻醉方法名称" varchar (1000) DEFAULT NULL,
 "拟实施麻醉方法代码" varchar (20) DEFAULT NULL,
 "拟实施手术及操作时间" timestamp DEFAULT NULL,
 CONSTRAINT "术前小结"_"医疗机构代码"_"术前小结流水号"_PK PRIMARY KEY ("医疗机构代码",
 "术前小结流水号")
);


COMMENT ON TABLE "材料目录" IS '医疗机构内材料的分类、材质、型号、生产企业等基本信息';
COMMENT ON COLUMN "材料目录"."商品名" IS '材料的商品名';
COMMENT ON COLUMN "材料目录"."材料分类代码" IS '材料分类(如医用工具类、置入类材料、植入材料类、口腔材料等)在特定编码体系中的代码';
COMMENT ON COLUMN "材料目录"."材料包装单位" IS '采购、运输过程中的最小包装单位，如箱';
COMMENT ON COLUMN "材料目录"."医保项目代码" IS '该医用材料在医保目录编码规则下的代码';
COMMENT ON COLUMN "材料目录"."医保项目名称" IS '该医用材料在医保目录编码规则下的名称';
COMMENT ON COLUMN "材料目录"."规格" IS '根据质量标准标识的耗材规格名称';
COMMENT ON COLUMN "材料目录"."型号" IS '医用材料的规格型号';
COMMENT ON COLUMN "材料目录"."零售单价" IS '材料的零售单价';
COMMENT ON COLUMN "材料目录"."成本单价" IS '材料的成本单价';
COMMENT ON COLUMN "材料目录"."注册证产品名称" IS '材料在注册证上的产品名称';
COMMENT ON COLUMN "材料目录"."注册证号" IS '该类耗材在各地食药监局注册/备案的证件编号';
COMMENT ON COLUMN "材料目录"."注册日期" IS '注册证注册当日的公元纪年日期';
COMMENT ON COLUMN "材料目录"."注册证截止日期" IS '注册证有效期截止当日的公元纪年日期';
COMMENT ON COLUMN "材料目录"."品牌" IS '不同厂家的耗材识别标志';
COMMENT ON COLUMN "材料目录"."最小医院等级" IS '具备使用该收费项目的最小医院等级';
COMMENT ON COLUMN "材料目录"."最小医师等级" IS '具备使用该收费项目的最小医师等级';
COMMENT ON COLUMN "材料目录"."限制适用性别" IS '该药品仅适用性别描述';
COMMENT ON COLUMN "材料目录"."适用范围" IS '该医用材料的适用范围描述';
COMMENT ON COLUMN "材料目录"."材料内涵" IS '对医用材料特别包含内容的详细描述';
COMMENT ON COLUMN "材料目录"."材料说明" IS '医用材料的详细描述';
COMMENT ON COLUMN "材料目录"."材料除外内容" IS '对医用材料所不包含内容的详细描述';
COMMENT ON COLUMN "材料目录"."收费项目等级" IS '医用材料的医保等级代码';
COMMENT ON COLUMN "材料目录"."最小计量单位" IS '是指产品临床使用时的最小计量单位，如：1 根/包的“根”，其中每根为最小计量单位';
COMMENT ON COLUMN "材料目录"."最小销售包装单位" IS '是指产品销售时的最小计量单位，如：1 根/包的“包”，拆零包装不属于最小销售包装';
COMMENT ON COLUMN "材料目录"."转换系数" IS '最小销售包装包含最新小计量单位的数量';
COMMENT ON COLUMN "材料目录"."最小销售包装进货价格" IS '医疗机构购买最小销售包装的进货价(元)，保留2位小数';
COMMENT ON COLUMN "材料目录"."最小计量单位进货价格" IS '最小销售包装进货价/转换系数(元)，保留4位小数';
COMMENT ON COLUMN "材料目录"."最小销售包装进货数量" IS '医疗机构购买最小销售包装的进货数量';
COMMENT ON COLUMN "材料目录"."最小计量单位进货数量" IS '最小销售包装进货数量*转换系数';
COMMENT ON COLUMN "材料目录"."最小销售包装销售价格" IS '卖给患者的最小销售包装价格(元)，保留2位小数';
COMMENT ON COLUMN "材料目录"."最小计量单位销售价格" IS '最小销售包装销售价格/转换系数(元)，保留4位小数';
COMMENT ON COLUMN "材料目录"."最小销售包装销售数量" IS '卖给患者最小销售包装的销售数量';
COMMENT ON COLUMN "材料目录"."最小计量单位销售数量" IS '最小销售包装销售数量*转换系数';
COMMENT ON COLUMN "材料目录"."植入材料和人体器官标志" IS '标识该材料是否为植入材料和人体器官';
COMMENT ON COLUMN "材料目录"."高值耗材标志" IS '标识是否为高值耗材的标识';
COMMENT ON COLUMN "材料目录"."医保报销标志" IS '标识是否为医保报销范围的标志';
COMMENT ON COLUMN "材料目录"."植入物标志" IS '标志是否为植入物的标识';
COMMENT ON COLUMN "材料目录"."按批管理标志" IS '标识是否为按批管理的标志';
COMMENT ON COLUMN "材料目录"."耐用品标志" IS '标识是否为耐用品的标志';
COMMENT ON COLUMN "材料目录"."条码管理标志" IS '标识是否为条码管理的标志';
COMMENT ON COLUMN "材料目录"."收费标志" IS '标识是否为收费的标志';
COMMENT ON COLUMN "材料目录"."停用标志" IS '标识材料是否停用的标志';
COMMENT ON COLUMN "材料目录"."停用时间" IS '停用某材料当天的公元纪年日期的完整描述';
COMMENT ON COLUMN "材料目录"."基准价格" IS '收费项目的基础收费价格，计量单位为人民币元';
COMMENT ON COLUMN "材料目录"."最高限价" IS '收费项目限制的最高收费价格，计量单位为人民币元';
COMMENT ON COLUMN "材料目录"."自付比例" IS '乙类项目的自付比例，甲类0，丙类1';
COMMENT ON COLUMN "材料目录"."计价单位" IS '医用耗材的计价单位名称';
COMMENT ON COLUMN "材料目录"."供应商名称" IS '供应商在工商局注册、审批通过后的名称';
COMMENT ON COLUMN "材料目录"."供应商代码" IS '供应商按照医疗机构内特定编码规则赋予企业的顺序号';
COMMENT ON COLUMN "材料目录"."生产批号" IS '按照某一特定编码规则赋予药品生产批号的唯一标志';
COMMENT ON COLUMN "材料目录"."生产厂家名称" IS '生产该类耗材的在工商局注册、审批通过后的厂家名称';
COMMENT ON COLUMN "材料目录"."生产地类别" IS '标识该材料是进口或者国产的类别名称';
COMMENT ON COLUMN "材料目录"."产地" IS '药品生产地的地址';
COMMENT ON COLUMN "材料目录"."生产时间" IS '药品生产完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "材料目录"."有效期截止日期" IS '在一定的贮存条件下，能够保持质量的有效期限截止日期。药品、耗材需要填写';
COMMENT ON COLUMN "材料目录"."记录状态" IS '标识记录是否正常使用的标志，其中：0：正常；1：停用';
COMMENT ON COLUMN "材料目录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "材料目录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "材料目录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "材料目录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "材料目录"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "材料目录"."材料代码" IS '医疗机构内材料代码';
COMMENT ON COLUMN "材料目录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "材料目录"."材料名称" IS '医疗机构内材料名称';
COMMENT ON COLUMN "材料目录"."平台材料代码" IS '平台中心材料代码';
CREATE TABLE IF NOT EXISTS "材料目录" (
"商品名" varchar (200) DEFAULT NULL,
 "材料分类代码" varchar (36) DEFAULT NULL,
 "材料包装单位" varchar (32) DEFAULT NULL,
 "医保项目代码" varchar (53) DEFAULT NULL,
 "医保项目名称" varchar (300) DEFAULT NULL,
 "规格" varchar (255) DEFAULT NULL,
 "型号" varchar (300) DEFAULT NULL,
 "零售单价" decimal (10,
 2) DEFAULT NULL,
 "成本单价" decimal (10,
 2) DEFAULT NULL,
 "注册证产品名称" varchar (200) DEFAULT NULL,
 "注册证号" varchar (20) DEFAULT NULL,
 "注册日期" date DEFAULT NULL,
 "注册证截止日期" date DEFAULT NULL,
 "品牌" varchar (100) DEFAULT NULL,
 "最小医院等级" varchar (5) DEFAULT NULL,
 "最小医师等级" varchar (5) DEFAULT NULL,
 "限制适用性别" varchar (5) DEFAULT NULL,
 "适用范围" varchar (500) DEFAULT NULL,
 "材料内涵" varchar (500) DEFAULT NULL,
 "材料说明" varchar (500) DEFAULT NULL,
 "材料除外内容" varchar (500) DEFAULT NULL,
 "收费项目等级" decimal (10,
 2) DEFAULT NULL,
 "最小计量单位" varchar (50) DEFAULT NULL,
 "最小销售包装单位" varchar (50) DEFAULT NULL,
 "转换系数" decimal (10,
 2) DEFAULT NULL,
 "最小销售包装进货价格" decimal (10,
 2) DEFAULT NULL,
 "最小计量单位进货价格" decimal (10,
 2) DEFAULT NULL,
 "最小销售包装进货数量" decimal (10,
 2) DEFAULT NULL,
 "最小计量单位进货数量" decimal (10,
 2) DEFAULT NULL,
 "最小销售包装销售价格" decimal (10,
 2) DEFAULT NULL,
 "最小计量单位销售价格" decimal (10,
 2) DEFAULT NULL,
 "最小销售包装销售数量" decimal (10,
 2) DEFAULT NULL,
 "最小计量单位销售数量" decimal (10,
 2) DEFAULT NULL,
 "植入材料和人体器官标志" varchar (1) DEFAULT NULL,
 "高值耗材标志" varchar (1) DEFAULT NULL,
 "医保报销标志" varchar (1) DEFAULT NULL,
 "植入物标志" varchar (1) DEFAULT NULL,
 "按批管理标志" varchar (1) DEFAULT NULL,
 "耐用品标志" varchar (1) DEFAULT NULL,
 "条码管理标志" varchar (1) DEFAULT NULL,
 "收费标志" varchar (1) DEFAULT NULL,
 "停用标志" varchar (1) DEFAULT NULL,
 "停用时间" timestamp DEFAULT NULL,
 "基准价格" decimal (10,
 2) DEFAULT NULL,
 "最高限价" decimal (10,
 2) DEFAULT NULL,
 "自付比例" decimal (10,
 2) DEFAULT NULL,
 "计价单位" varchar (75) DEFAULT NULL,
 "供应商名称" varchar (100) DEFAULT NULL,
 "供应商代码" varchar (64) DEFAULT NULL,
 "生产批号" varchar (32) DEFAULT NULL,
 "生产厂家名称" varchar (70) DEFAULT NULL,
 "生产地类别" varchar (32) DEFAULT NULL,
 "产地" varchar (100) DEFAULT NULL,
 "生产时间" timestamp DEFAULT NULL,
 "有效期截止日期" date DEFAULT NULL,
 "记录状态" varchar (1) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "材料代码" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "材料名称" varchar (100) DEFAULT NULL,
 "平台材料代码" varchar (64) DEFAULT NULL,
 CONSTRAINT "材料目录"_"医疗机构代码"_"材料代码"_PK PRIMARY KEY ("医疗机构代码",
 "材料代码")
);


COMMENT ON TABLE "检验收费项目表" IS '本次检验活动，检验套餐与检验收费项目的映射表';
COMMENT ON COLUMN "检验收费项目表"."记录状态" IS '记录状态的描述，其中：0：正常，1：停用';
COMMENT ON COLUMN "检验收费项目表"."平台收费项目名称" IS '物价收费项目在平台中心特定编码体系中的标准名称';
COMMENT ON COLUMN "检验收费项目表"."平台收费项目代码" IS '物价收费项目在平台中心特定编码体系中的代码';
COMMENT ON COLUMN "检验收费项目表"."收费项目名称" IS '物价收费项目在机构内编码体系中的名称';
COMMENT ON COLUMN "检验收费项目表"."收费项目代码" IS '物价收费项目在机构内编码体系中的代码';
COMMENT ON COLUMN "检验收费项目表"."临床项目名称" IS '医院检验套餐名称，如果是小项目则传具体检验项目名称';
COMMENT ON COLUMN "检验收费项目表"."临床项目代码" IS '医院检验套餐编码，如果是小项目则传具体检验项目编码';
COMMENT ON COLUMN "检验收费项目表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "检验收费项目表"."检验收费项目流水号" IS '按照某一特性编码规则赋予检验收费项目记录唯一标志的顺序号';
COMMENT ON COLUMN "检验收费项目表"."报告时间" IS '报告生成当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "检验收费项目表"."检验报告流水号" IS '按照某一特定编码规则赋予检验报告记录的唯一标识，在同一家医院内唯一';
COMMENT ON COLUMN "检验收费项目表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "检验收费项目表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "检验收费项目表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "检验收费项目表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "检验收费项目表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
CREATE TABLE IF NOT EXISTS "检验收费项目表" (
"记录状态" varchar (1) DEFAULT NULL,
 "平台收费项目名称" varchar (100) DEFAULT NULL,
 "平台收费项目代码" varchar (64) DEFAULT NULL,
 "收费项目名称" varchar (100) DEFAULT NULL,
 "收费项目代码" varchar (64) DEFAULT NULL,
 "临床项目名称" varchar (200) DEFAULT NULL,
 "临床项目代码" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "检验收费项目流水号" varchar (64) NOT NULL,
 "报告时间" timestamp NOT NULL,
 "检验报告流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 CONSTRAINT "检验收费项目表"_"检验收费项目流水号"_"报告时间"_"检验报告流水号"_"医疗机构代码"_PK PRIMARY KEY ("检验收费项目流水号",
 "报告时间",
 "检验报告流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "死亡医学证明" IS '居民死亡相关信息记录';
COMMENT ON COLUMN "死亡医学证明"."直接死亡原因名称B" IS '直接导致患者死亡的最终疾病或原因B的标准名称';
COMMENT ON COLUMN "死亡医学证明"."现住址-县(市、区)代码" IS '现住地址中的县或区在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."现住址-市(地区、州)名称" IS '现住地址中的市、地区或州的名称';
COMMENT ON COLUMN "死亡医学证明"."现住址-市(地区、州)代码" IS '现住地址中的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."现住址-省(自治区、直辖市)名称" IS '现住地址中的省、自治区或直辖市名称';
COMMENT ON COLUMN "死亡医学证明"."现住址-省(自治区、直辖市)代码" IS '现住地址中的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."现住址行政区划代码" IS '现住地址所在区划的行政区划代码';
COMMENT ON COLUMN "死亡医学证明"."现住详细地址" IS '现住地址的详细描述';
COMMENT ON COLUMN "死亡医学证明"."户籍地-门牌号码" IS '户籍登记所在地址的门牌号码';
COMMENT ON COLUMN "死亡医学证明"."户籍地-村(街、路、弄等)名称" IS '户籍登记所在地址的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "死亡医学证明"."户籍地-村(街、路、弄等)代码" IS '户籍登记所在地址的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."户籍地-乡(镇、街道办事处)名称" IS '户籍登记所在地址的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "死亡医学证明"."户籍地-乡(镇、街道办事处)代码" IS '户籍登记所在地址的乡、镇或城市的街道办事处在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."户籍地-县(市、区)名称" IS '户籍登记所在地址的县(市、区)的名称';
COMMENT ON COLUMN "死亡医学证明"."户籍地-县(市、区)代码" IS '户籍登记所在地址的县(区)的在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."户籍地-市(地区、州)名称" IS '户籍登记所在地址的市、地区或州的名称';
COMMENT ON COLUMN "死亡医学证明"."户籍地-市(地区、州)代码" IS '户籍登记所在地址的市、地区或州的在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."户籍地-省(自治区、直辖市)名称" IS '户籍登记所在地址的省、自治区或直辖市名称';
COMMENT ON COLUMN "死亡医学证明"."户籍地-省(自治区、直辖市)代码" IS '户籍登记所在地址的省、自治区或直辖市在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."户籍地-行政区划代码" IS '户籍地址所在区划的行政区划代码';
COMMENT ON COLUMN "死亡医学证明"."户籍地-详细地址" IS '户籍地址的详细描述';
COMMENT ON COLUMN "死亡医学证明"."发病到死亡时长B" IS '发病到死亡的时间间隔B,计量单位包括岁、月、天、小时、分钟';
COMMENT ON COLUMN "死亡医学证明"."联系人姓名" IS '联系人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡医学证明"."实足年龄" IS '患者的实足年龄，为按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "死亡医学证明"."学历名称" IS '个体受教育最高程度的类别(如研究生教育、大学本科、专科教育等)在特定编码体系中的名称';
COMMENT ON COLUMN "死亡医学证明"."学历代码" IS '个体受教育最高程度的类别(如研究生教育、大学本科、专科教育等)在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."婚姻状况名称" IS '当前婚姻状况(已婚、未婚、初婚等)在特定编码体系中的名称';
COMMENT ON COLUMN "死亡医学证明"."婚姻状况代码" IS '当前婚姻状况(已婚、未婚、初婚等)在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."职业类别名称" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的名称';
COMMENT ON COLUMN "死亡医学证明"."职业类别代码" IS '本人从事职业所属类别(如国家机关负责人、专业技术人员、办事和有关人员等)在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."民族名称" IS '所属民族在特定编码体系中的名称';
COMMENT ON COLUMN "死亡医学证明"."民族代码" IS '所属民族在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."工作单位联系电话" IS '工作单位联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "死亡医学证明"."工作单位名称" IS '个体工作单位的组织机构名称';
COMMENT ON COLUMN "死亡医学证明"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "死亡医学证明"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "死亡医学证明"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "死亡医学证明"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "死亡医学证明"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."本人姓名" IS '本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡医学证明"."住院就诊流水号" IS '按照某一特定编码规则赋予住院事件的唯一标识';
COMMENT ON COLUMN "死亡医学证明"."城乡居民健康档案编号" IS '按照某一特定编码规则赋予个体城乡居民健康档案的编号';
COMMENT ON COLUMN "死亡医学证明"."个人唯一标识号" IS '按照一定编码规则赋予个人的唯一标识';
COMMENT ON COLUMN "死亡医学证明"."死亡医学证明编号" IS '死亡医学证明的唯一标识';
COMMENT ON COLUMN "死亡医学证明"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "死亡医学证明"."证明流水号" IS '按照某一特定编码规则赋予死亡医学证明的顺序号';
COMMENT ON COLUMN "死亡医学证明"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "死亡医学证明"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "死亡医学证明"."直接死亡原因代码C" IS '直接导致患者死亡的最终疾病或原因C的标准代码';
COMMENT ON COLUMN "死亡医学证明"."直接死亡原因名称C" IS '直接导致患者死亡的最终疾病或原因C的标准名称';
COMMENT ON COLUMN "死亡医学证明"."发病到死亡时长C" IS '发病到死亡的时间间隔C,计量单位包括岁、月、天、小时、分钟';
COMMENT ON COLUMN "死亡医学证明"."直接死亡原因代码D" IS '直接导致患者死亡的最终疾病或原因D的标准代码';
COMMENT ON COLUMN "死亡医学证明"."直接死亡原因名称D" IS '直接导致患者死亡的最终疾病或原因D的标准名称';
COMMENT ON COLUMN "死亡医学证明"."发病到死亡时长D" IS '发病到死亡的时间间隔D,计量单位包括岁、月、天、小时、分钟';
COMMENT ON COLUMN "死亡医学证明"."其他疾病诊断代码1" IS '促进死亡，但与导致死亡的疾病或情况无关的其他重要诊断1在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."其他疾病诊断名称1" IS '促进死亡，但与导致死亡的疾病或情况无关的其他重要诊断1在特定编码体系中的名称';
COMMENT ON COLUMN "死亡医学证明"."其他疾病诊断名称2" IS '促进死亡，但与导致死亡的疾病或情况无关的其他重要诊断2在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."其他疾病诊断代码2" IS '促进死亡，但与导致死亡的疾病或情况无关的其他重要诊断2在特定编码体系中的名称';
COMMENT ON COLUMN "死亡医学证明"."其他疾病诊断名称3" IS '促进死亡，但与导致死亡的疾病或情况无关的其他重要诊断3在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."其他疾病诊断代码3" IS '促进死亡，但与导致死亡的疾病或情况无关的其他重要诊断3在特定编码体系中的名称';
COMMENT ON COLUMN "死亡医学证明"."最高诊断机构级别代码" IS '对死亡进行最高效力诊断的机构的级别类别(如省级、地区级、县级、乡镇卫生院等)在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."最高诊断机构级别名称" IS '对死亡进行最高效力诊断的机构的级别类别的标准名称，如省级、地区级、县级、乡镇卫生院等';
COMMENT ON COLUMN "死亡医学证明"."死亡最高诊断依据代码" IS '对根本死因诊断的最高依据类别(如尸检、病理、手术、临床、理化等)在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."死亡最高诊断依据名称" IS '对根本死因诊断的最高依据类别的标准名称，如尸检、病理、手术、临床、理化等';
COMMENT ON COLUMN "死亡医学证明"."根本死因代码" IS '导致本人死亡的最根本疾病的西医诊断标准代码';
COMMENT ON COLUMN "死亡医学证明"."根本死因名称" IS '导致本人死亡的最根本疾病的西医诊断标准名称';
COMMENT ON COLUMN "死亡医学证明"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡医学证明"."登记人员工号" IS '登记人员的工号';
COMMENT ON COLUMN "死亡医学证明"."登记人员姓名" IS '登记人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡医学证明"."登记机构代码" IS '登记机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "死亡医学证明"."登记机构名称" IS '登记机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "死亡医学证明"."修改时间" IS '完成修改时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡医学证明"."修改人员工号" IS '修改人员的工号';
COMMENT ON COLUMN "死亡医学证明"."修改人员姓名" IS '修改人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡医学证明"."修改机构代码" IS '修改机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "死亡医学证明"."修改机构名称" IS '修改机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "死亡医学证明"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "死亡医学证明"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡医学证明"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡医学证明"."现住址-县(市、区)名称" IS '现住地址中的县或区名称';
COMMENT ON COLUMN "死亡医学证明"."家人电话号码" IS '家人联系电话的号码，包括国际、国内区号和分机号';
COMMENT ON COLUMN "死亡医学证明"."直接死亡原因代码B" IS '直接导致患者死亡的最终疾病或原因B的标准代码';
COMMENT ON COLUMN "死亡医学证明"."发病到死亡时长A" IS '发病到死亡的时间间隔A,计量单位包括岁、月、天、小时、分钟';
COMMENT ON COLUMN "死亡医学证明"."直接死亡原因名称A" IS '直接导致患者死亡的最终疾病或原因A的标准名称';
COMMENT ON COLUMN "死亡医学证明"."直接死亡原因代码A" IS '直接导致患者死亡的最终疾病或原因A的标准代码';
COMMENT ON COLUMN "死亡医学证明"."死亡医院名称" IS '死亡医院的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "死亡医学证明"."死亡医院代码" IS '按照某一特定编码规则赋予死亡医院的唯一标识。这里指医疗机构组织机构代码';
COMMENT ON COLUMN "死亡医学证明"."死亡地点代码" IS '死亡时所在地点的类别(如医院病房、急诊室、家中、外地、家庭病床等)在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."死亡日期" IS '患者死亡当日的公元纪年日期';
COMMENT ON COLUMN "死亡医学证明"."填报机构名称" IS '填报机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "死亡医学证明"."填报机构代码" IS '按照某一特定编码规则赋予填报机构的唯一标识。这里指医疗机构组织机构代码';
COMMENT ON COLUMN "死亡医学证明"."填报人姓名" IS '填报人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡医学证明"."填报(表)日期" IS '完成填写及上报时的公元纪年日期';
COMMENT ON COLUMN "死亡医学证明"."现住址邮编" IS '现住地址中所在行政区的邮政编码';
COMMENT ON COLUMN "死亡医学证明"."现住址-门牌号码" IS '本人现住地址中的门牌号码';
COMMENT ON COLUMN "死亡医学证明"."现住址-村(街、路、弄等)名称" IS '本人现住地址中的村或城市的街、路、里、弄等名称';
COMMENT ON COLUMN "死亡医学证明"."现住址-村(街、路、弄等)代码" IS '本人现住地址中的村或城市的街、路、里、弄等在特定编码体系中的代码';
COMMENT ON COLUMN "死亡医学证明"."现住址-乡(镇、街道办事处)名称" IS '本人现住地址中的乡、镇或城市的街道办事处名称';
COMMENT ON COLUMN "死亡医学证明"."现地址-乡(镇、街道办事处)代码" IS '本人现住地址中的乡、镇或城市的街道办事处在特定编码体系中的代码';
CREATE TABLE IF NOT EXISTS "死亡医学证明" (
"直接死亡原因名称B" varchar (512) DEFAULT NULL,
 "现住址-县(市、区)代码" varchar (20) DEFAULT NULL,
 "现住址-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "现住址-市(地区、州)代码" varchar (6) DEFAULT NULL,
 "现住址-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "现住址-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "现住址行政区划代码" varchar (12) DEFAULT NULL,
 "现住详细地址" varchar (128) DEFAULT NULL,
 "户籍地-门牌号码" varchar (100) DEFAULT NULL,
 "户籍地-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "户籍地-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "户籍地-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 "户籍地-县(市、区)名称" varchar (70) DEFAULT NULL,
 "户籍地-县(市、区)代码" varchar (6) DEFAULT NULL,
 "户籍地-市(地区、州)名称" varchar (70) DEFAULT NULL,
 "户籍地-市(地区、州)代码" varchar (4) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)名称" varchar (70) DEFAULT NULL,
 "户籍地-省(自治区、直辖市)代码" varchar (2) DEFAULT NULL,
 "户籍地-行政区划代码" varchar (12) DEFAULT NULL,
 "户籍地-详细地址" varchar (200) DEFAULT NULL,
 "发病到死亡时长B" varchar (20) DEFAULT NULL,
 "联系人姓名" varchar (50) DEFAULT NULL,
 "实足年龄" decimal (6,
 0) DEFAULT NULL,
 "学历名称" varchar (50) DEFAULT NULL,
 "学历代码" varchar (5) DEFAULT NULL,
 "婚姻状况名称" varchar (50) DEFAULT NULL,
 "婚姻状况代码" varchar (2) DEFAULT NULL,
 "职业类别名称" varchar (60) DEFAULT NULL,
 "职业类别代码" varchar (4) DEFAULT NULL,
 "民族名称" varchar (50) DEFAULT NULL,
 "民族代码" varchar (2) DEFAULT NULL,
 "工作单位联系电话" varchar (20) DEFAULT NULL,
 "工作单位名称" varchar (70) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "本人姓名" varchar (50) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "城乡居民健康档案编号" varchar (17) DEFAULT NULL,
 "个人唯一标识号" varchar (64) DEFAULT NULL,
 "死亡医学证明编号" varchar (10) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "证明流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "直接死亡原因代码C" varchar (20) DEFAULT NULL,
 "直接死亡原因名称C" varchar (512) DEFAULT NULL,
 "发病到死亡时长C" varchar (20) DEFAULT NULL,
 "直接死亡原因代码D" varchar (20) DEFAULT NULL,
 "直接死亡原因名称D" varchar (512) DEFAULT NULL,
 "发病到死亡时长D" varchar (20) DEFAULT NULL,
 "其他疾病诊断代码1" varchar (20) DEFAULT NULL,
 "其他疾病诊断名称1" varchar (200) DEFAULT NULL,
 "其他疾病诊断名称2" varchar (200) DEFAULT NULL,
 "其他疾病诊断代码2" varchar (20) DEFAULT NULL,
 "其他疾病诊断名称3" varchar (200) DEFAULT NULL,
 "其他疾病诊断代码3" varchar (20) DEFAULT NULL,
 "最高诊断机构级别代码" varchar (2) DEFAULT NULL,
 "最高诊断机构级别名称" varchar (70) DEFAULT NULL,
 "死亡最高诊断依据代码" varchar (2) DEFAULT NULL,
 "死亡最高诊断依据名称" varchar (200) DEFAULT NULL,
 "根本死因代码" varchar (20) DEFAULT NULL,
 "根本死因名称" varchar (200) DEFAULT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "登记人员工号" varchar (20) DEFAULT NULL,
 "登记人员姓名" varchar (50) DEFAULT NULL,
 "登记机构代码" varchar (22) DEFAULT NULL,
 "登记机构名称" varchar (70) DEFAULT NULL,
 "修改时间" timestamp DEFAULT NULL,
 "修改人员工号" varchar (20) DEFAULT NULL,
 "修改人员姓名" varchar (50) DEFAULT NULL,
 "修改机构代码" varchar (22) DEFAULT NULL,
 "修改机构名称" varchar (70) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "现住址-县(市、区)名称" varchar (70) DEFAULT NULL,
 "家人电话号码" varchar (20) DEFAULT NULL,
 "直接死亡原因代码B" varchar (20) DEFAULT NULL,
 "发病到死亡时长A" varchar (20) DEFAULT NULL,
 "直接死亡原因名称A" varchar (512) DEFAULT NULL,
 "直接死亡原因代码A" varchar (20) DEFAULT NULL,
 "死亡医院名称" varchar (70) DEFAULT NULL,
 "死亡医院代码" varchar (22) DEFAULT NULL,
 "死亡地点代码" varchar (2) DEFAULT NULL,
 "死亡日期" date DEFAULT NULL,
 "填报机构名称" varchar (70) DEFAULT NULL,
 "填报机构代码" varchar (22) DEFAULT NULL,
 "填报人姓名" varchar (50) DEFAULT NULL,
 "填报(表)日期" date DEFAULT NULL,
 "现住址邮编" varchar (6) DEFAULT NULL,
 "现住址-门牌号码" varchar (70) DEFAULT NULL,
 "现住址-村(街、路、弄等)名称" varchar (70) DEFAULT NULL,
 "现住址-村(街、路、弄等)代码" varchar (20) DEFAULT NULL,
 "现住址-乡(镇、街道办事处)名称" varchar (70) DEFAULT NULL,
 "现地址-乡(镇、街道办事处)代码" varchar (9) DEFAULT NULL,
 CONSTRAINT "死亡医学证明"_"证明流水号"_"医疗机构代码"_PK PRIMARY KEY ("证明流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "死亡记录" IS '患者死亡时的记录，包括入院时间、入院诊断、诊疗过程、直接死亡原因等';
COMMENT ON COLUMN "死亡记录"."文本内容" IS '存入大文本的内容，最大不超过64K';
COMMENT ON COLUMN "死亡记录"."签名时间" IS '医师姓名完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡记录"."主任医师姓名" IS '主任医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡记录"."主任医师工号" IS '主任医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "死亡记录"."主治医师姓名" IS '患者出院时所在科室的具有主治医师专业技术职务资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡记录"."主治医师工号" IS '所在科室的具有主治医师的工号';
COMMENT ON COLUMN "死亡记录"."住院医师姓名" IS '所在科室具体负责诊治的，具有住院医师专业技术职务任职资格的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡记录"."住院医师工号" IS '所在科室具体负责诊治的，具有住院医师的工号';
COMMENT ON COLUMN "死亡记录"."家属同意尸体解剖标志标志" IS '标识是否对死亡患者的机体进行剖验，以明确死亡原因的标志';
COMMENT ON COLUMN "死亡记录"."死亡诊断名称" IS '导致患者死亡的西医疾病诊断标准名称,如果有多个疾病诊断,这里指与其他疾病有因果关系的,并因其发生发展引起其他疾病,最终导致死亡的一系列疾病诊断中最初确定的疾病诊断名称';
COMMENT ON COLUMN "死亡记录"."死亡诊断代码" IS '按照平台内编码规则赋予死亡诊断疾病的唯一标识';
COMMENT ON COLUMN "死亡记录"."直接死亡原因名称" IS '直接导致患者死亡的最终疾病或原因的名称';
COMMENT ON COLUMN "死亡记录"."直接死亡原因代码" IS '按照平台编码规则赋予直接死因的唯一标识';
COMMENT ON COLUMN "死亡记录"."死亡时间" IS '死亡时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡记录"."诊疗过程描述" IS '对患者诊疗过程或抢救情况的详细描述';
COMMENT ON COLUMN "死亡记录"."入院情况" IS '对患者入院情况的详细描述';
COMMENT ON COLUMN "死亡记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "死亡记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "死亡记录"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "死亡记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "死亡记录"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "死亡记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "死亡记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "死亡记录"."入院诊断-西医诊断名称" IS '由医师根据患者入院时的情况，综合分析所作出的西医诊断标准名称';
COMMENT ON COLUMN "死亡记录"."入院诊断-西医诊断代码" IS '患者入院时按照平台编码规则赋予西医初步诊断疾病的唯一标识';
COMMENT ON COLUMN "死亡记录"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "死亡记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "死亡记录"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "死亡记录"."科室名称" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "死亡记录"."科室代码" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "死亡记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "死亡记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "死亡记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "死亡记录"."住院次数" IS '办理完整住院治疗手续的次数';
COMMENT ON COLUMN "死亡记录"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "死亡记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "死亡记录"."住院死亡记录流水号" IS '按照某一特性编码规则赋予住院死亡记录的唯一标识';
COMMENT ON COLUMN "死亡记录"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "死亡记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "死亡记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "死亡记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
CREATE TABLE IF NOT EXISTS "死亡记录" (
"文本内容" text,
 "签名时间" timestamp DEFAULT NULL,
 "主任医师姓名" varchar (50) DEFAULT NULL,
 "主任医师工号" varchar (20) DEFAULT NULL,
 "主治医师姓名" varchar (50) DEFAULT NULL,
 "主治医师工号" varchar (20) DEFAULT NULL,
 "住院医师姓名" varchar (50) DEFAULT NULL,
 "住院医师工号" varchar (20) DEFAULT NULL,
 "家属同意尸体解剖标志标志" varchar (1) DEFAULT NULL,
 "死亡诊断名称" varchar (512) DEFAULT NULL,
 "死亡诊断代码" varchar (64) DEFAULT NULL,
 "直接死亡原因名称" varchar (512) DEFAULT NULL,
 "直接死亡原因代码" varchar (20) DEFAULT NULL,
 "死亡时间" timestamp DEFAULT NULL,
 "诊疗过程描述" text,
 "入院情况" text,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "入院诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "入院诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "住院死亡记录流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 CONSTRAINT "死亡记录"_"住院死亡记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("住院死亡记录流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "治疗记录" IS '患者治疗记录，包括操作名称、时间和随访周期，执行医师';
COMMENT ON COLUMN "治疗记录"."操作方法描述" IS '操作方法的详细描述';
COMMENT ON COLUMN "治疗记录"."操作次数" IS '实施操作的次数';
COMMENT ON COLUMN "治疗记录"."操作时间" IS '对患者实施的操作结束时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "治疗记录"."过敏史标志" IS '标识患者有无过敏经历的标志';
COMMENT ON COLUMN "治疗记录"."过敏史" IS '患者既往发生过敏情况的详细描述';
COMMENT ON COLUMN "治疗记录"."医嘱使用备注" IS '医嘱执行过程中的注意事项';
COMMENT ON COLUMN "治疗记录"."今后治疗方案" IS '今后治疗方案的详细描述';
COMMENT ON COLUMN "治疗记录"."医嘱执行者姓名" IS '医嘱执行人员在公安机关登记的姓氏与名字';
COMMENT ON COLUMN "治疗记录"."医嘱执行者工号" IS '医嘱执行人员在机构内编码体系中的编号';
COMMENT ON COLUMN "治疗记录"."中药使用类别代码" IS '中药使用类别(如未使用、中成药、中草药、其他中药)在特定编码体系中的代码';
COMMENT ON COLUMN "治疗记录"."随访方式代码" IS '随访方式的特定类型在特定编码体系中的代码';
COMMENT ON COLUMN "治疗记录"."随访方式名称" IS '随访方式的特定类型在特定编码体系中的名称';
COMMENT ON COLUMN "治疗记录"."随访日期" IS '对患者进行随访时当日的公元纪年日期的完整描述';
COMMENT ON COLUMN "治疗记录"."随访周期建议代码" IS '建议患者接受医学随访的间隔时长(如每2周、每半年、每年)在特定编码体系中的代码';
COMMENT ON COLUMN "治疗记录"."随访周期建议名称" IS '建议患者接受医学随访的间隔时长在特定编码体系中的名称，如每2周、每半年、每年';
COMMENT ON COLUMN "治疗记录"."数据生成时间" IS '数据生成当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "治疗记录"."签名时间" IS '签名日期当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "治疗记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "治疗记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "治疗记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "治疗记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "治疗记录"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "治疗记录"."治疗记录流水号" IS '按照某一特定编码规则赋予治疗记录的唯一标识';
COMMENT ON COLUMN "治疗记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "治疗记录"."电子申请单编号" IS '按照某一特定编码规则赋予电子申请单的顺序号';
COMMENT ON COLUMN "治疗记录"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
COMMENT ON COLUMN "治疗记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "治疗记录"."门诊就诊流水号" IS '为门诊就诊时，填写门诊就诊流水号，住院就诊流水号填“-”';
COMMENT ON COLUMN "治疗记录"."就诊次数" IS '对于门(急)诊患者，此处表示患者门(急)诊的累计次数；对于住院患者，此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "治疗记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "治疗记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "治疗记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "治疗记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "治疗记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "治疗记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "治疗记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "治疗记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "治疗记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "治疗记录"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "治疗记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "治疗记录"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "治疗记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "治疗记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "治疗记录"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "治疗记录"."疾病诊断代码" IS '疾病诊断在特定编码体系中的代码';
COMMENT ON COLUMN "治疗记录"."疾病诊断名称" IS '疾病诊断在特定编码体系中的名称';
COMMENT ON COLUMN "治疗记录"."处理及指导意见" IS '对某事件进行处理及指导意见内容的详细描述';
COMMENT ON COLUMN "治疗记录"."有创诊疗操作标志" IS '标识患者是否接受过有创诊疗操作的标志';
COMMENT ON COLUMN "治疗记录"."操作代码" IS '实施的治疗操作在特定编码体系中的代码';
COMMENT ON COLUMN "治疗记录"."操作名称" IS '实施的治疗操作在特定编码体系中的名称';
COMMENT ON COLUMN "治疗记录"."操作部位名称" IS '实施治疗操作的人体目标部位的名称，如双侧鼻孔、臀部、左臂、右眼等';
COMMENT ON COLUMN "治疗记录"."介入物名称" IS '实施手术操作时使用/放置的材料/药物的名称';
CREATE TABLE IF NOT EXISTS "治疗记录" (
"操作方法描述" text,
 "操作次数" decimal (3,
 0) DEFAULT NULL,
 "操作时间" timestamp DEFAULT NULL,
 "过敏史标志" varchar (1) DEFAULT NULL,
 "过敏史" text,
 "医嘱使用备注" varchar (100) DEFAULT NULL,
 "今后治疗方案" text,
 "医嘱执行者姓名" varchar (50) DEFAULT NULL,
 "医嘱执行者工号" varchar (20) DEFAULT NULL,
 "中药使用类别代码" varchar (1) DEFAULT NULL,
 "随访方式代码" varchar (2) DEFAULT NULL,
 "随访方式名称" varchar (50) DEFAULT NULL,
 "随访日期" date DEFAULT NULL,
 "随访周期建议代码" varchar (2) DEFAULT NULL,
 "随访周期建议名称" varchar (50) DEFAULT NULL,
 "数据生成时间" timestamp DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "治疗记录流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "电子申请单编号" varchar (100) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "门诊就诊流水号" varchar (32) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "疾病诊断名称" varchar (512) DEFAULT NULL,
 "处理及指导意见" text,
 "有创诊疗操作标志" varchar (1) DEFAULT NULL,
 "操作代码" varchar (50) DEFAULT NULL,
 "操作名称" varchar (80) DEFAULT NULL,
 "操作部位名称" varchar (50) DEFAULT NULL,
 "介入物名称" varchar (100) DEFAULT NULL,
 CONSTRAINT "治疗记录"_"医疗机构代码"_"治疗记录流水号"_PK PRIMARY KEY ("医疗机构代码",
 "治疗记录流水号")
);


COMMENT ON TABLE "生命体征测量记录" IS '患者生命体征测量信息，包括呼吸频率、体温、体重、收缩压以及护理观察等指标结果';
COMMENT ON COLUMN "生命体征测量记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "生命体征测量记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "生命体征测量记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "生命体征测量记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "生命体征测量记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "生命体征测量记录"."住院次数" IS '患者住院次数统计';
COMMENT ON COLUMN "生命体征测量记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "生命体征测量记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "生命体征测量记录"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "生命体征测量记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "生命体征测量记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "生命体征测量记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "生命体征测量记录"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "生命体征测量记录"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "生命体征测量记录"."实际住院天数" IS '患者实际的住院天数，入院日与出院日只计算1天';
COMMENT ON COLUMN "生命体征测量记录"."手术或分娩后天数" IS '截止当前记录日期为止，患者行手术或分娩后的天数';
COMMENT ON COLUMN "生命体征测量记录"."呼吸频率(次/min)" IS '受检者单位时间内呼吸的次数，计量单位为次/min';
COMMENT ON COLUMN "生命体征测量记录"."使用呼吸机标志" IS '标识患者是否使用呼吸机的标志';
COMMENT ON COLUMN "生命体征测量记录"."脉率(次/min)" IS '每分钟脉搏的次数测量值，计量单位为次/min';
COMMENT ON COLUMN "生命体征测量记录"."起搏器心率(次/MIN)" IS '使用起搏器时心脏搏动频率的测量值,计量单位为次/min';
COMMENT ON COLUMN "生命体征测量记录"."脉搏短绌标志" IS '标识患者是否出现脉搏短绌的标志';
COMMENT ON COLUMN "生命体征测量记录"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "生命体征测量记录"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "生命体征测量记录"."体温(℃)" IS '体温的测量值，计量单位为℃';
COMMENT ON COLUMN "生命体征测量记录"."体温表类型代码" IS '体温表类型在特定编码体系中的代码';
COMMENT ON COLUMN "生命体征测量记录"."体温表类型名称" IS '体温表类型在特定编码体系中的名称';
COMMENT ON COLUMN "生命体征测量记录"."物理降温标志" IS '标识是否对患者进行过物理降温的标志';
COMMENT ON COLUMN "生命体征测量记录"."降温处理后体温(℃)" IS '对问着进行物理降温后测得的体温值，计量单位为℃';
COMMENT ON COLUMN "生命体征测量记录"."身高(cm)" IS '个体身高的测量值，计量单位为cm';
COMMENT ON COLUMN "生命体征测量记录"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "生命体征测量记录"."腹围(cm)" IS '受检者腹部周长的测量值，计量单位为cm';
COMMENT ON COLUMN "生命体征测量记录"."疾病诊断代码" IS '疾病诊断在特定编码体系中的代码';
COMMENT ON COLUMN "生命体征测量记录"."疾病诊断名称" IS '疾病诊断在特定编码体系中的名称';
COMMENT ON COLUMN "生命体征测量记录"."护理观察项目名称" IS '护理观察项目的名称，如患者神志状态、饮食情况，皮肤情况、氧疗情况、排尿排便情况，流量、出量、人量等等，根据护理内容的不同选择不同的观察项目名称';
COMMENT ON COLUMN "生命体征测量记录"."护理观察结果" IS '对护理观察项目结果的详细描述';
COMMENT ON COLUMN "生命体征测量记录"."手术次数(次)" IS '患者进行手术的总次数';
COMMENT ON COLUMN "生命体征测量记录"."液体总入量(mL/日)" IS '液体每日入量的总体积，计量单位为mL/日';
COMMENT ON COLUMN "生命体征测量记录"."液体总出量(mL/日)" IS '液体每日出量的总体积，计量单位为mL/日';
COMMENT ON COLUMN "生命体征测量记录"."导尿标志" IS '标识是否导尿的标志';
COMMENT ON COLUMN "生命体征测量记录"."尿量(mL/日)" IS '患者每日尿量的总体积，计量单位为mL/日';
COMMENT ON COLUMN "生命体征测量记录"."其他液体排出量(mL/日)" IS '患者每日其他液体排出量的总体积，计量单位为mL/日';
COMMENT ON COLUMN "生命体征测量记录"."灌肠标志" IS '标识是否对患者进行灌肠的标志';
COMMENT ON COLUMN "生命体征测量记录"."大便失禁标志" IS '标识患者是否大便失禁的标志';
COMMENT ON COLUMN "生命体征测量记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "生命体征测量记录"."大便次数(次/日)" IS '患者每日大便总次数，计量单位为次/日';
COMMENT ON COLUMN "生命体征测量记录"."卧床标志" IS '标识患者是否卧床的标志';
COMMENT ON COLUMN "生命体征测量记录"."过敏源代码" IS '住院患者过敏源在特定编码体系中的代码';
COMMENT ON COLUMN "生命体征测量记录"."过敏源名称" IS '住院患者过敏源的具体名称';
COMMENT ON COLUMN "生命体征测量记录"."护士工号" IS '护理护士的工号';
COMMENT ON COLUMN "生命体征测量记录"."护士姓名" IS '护理护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "生命体征测量记录"."签名时间" IS '护理护士在护理记录上完成签名的公元纪年和日期的完整描述';
COMMENT ON COLUMN "生命体征测量记录"."审核人工号" IS '审核人员的工号';
COMMENT ON COLUMN "生命体征测量记录"."审核人姓名" IS '审核人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "生命体征测量记录"."审核时间" IS '审核完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "生命体征测量记录"."记录时间" IS '完成记录时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "生命体征测量记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "生命体征测量记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "生命体征测量记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "生命体征测量记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "生命体征测量记录"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "生命体征测量记录"."生命体征测量记录流水号" IS '按照某一特定编码规则赋予生命体征测量记录的顺序号';
CREATE TABLE IF NOT EXISTS "生命体征测量记录" (
"修改标志" varchar (1) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "实际住院天数" decimal (32,
 0) DEFAULT NULL,
 "手术或分娩后天数" decimal (32,
 0) DEFAULT NULL,
 "呼吸频率(次/min)" decimal (32,
 0) DEFAULT NULL,
 "使用呼吸机标志" varchar (1) DEFAULT NULL,
 "脉率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "起搏器心率(次/MIN)" decimal (32,
 0) DEFAULT NULL,
 "脉搏短绌标志" varchar (1) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "体温(℃)" decimal (3,
 1) DEFAULT NULL,
 "体温表类型代码" varchar (50) DEFAULT NULL,
 "体温表类型名称" varchar (50) DEFAULT NULL,
 "物理降温标志" varchar (1) DEFAULT NULL,
 "降温处理后体温(℃)" decimal (5,
 0) DEFAULT NULL,
 "身高(cm)" decimal (4,
 1) DEFAULT NULL,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "腹围(cm)" decimal (5,
 1) DEFAULT NULL,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "疾病诊断名称" varchar (512) DEFAULT NULL,
 "护理观察项目名称" varchar (200) DEFAULT NULL,
 "护理观察结果" text,
 "手术次数(次)" decimal (3,
 0) DEFAULT NULL,
 "液体总入量(mL/日)" decimal (10,
 3) DEFAULT NULL,
 "液体总出量(mL/日)" decimal (10,
 3) DEFAULT NULL,
 "导尿标志" varchar (1) DEFAULT NULL,
 "尿量(mL/日)" decimal (10,
 3) DEFAULT NULL,
 "其他液体排出量(mL/日)" decimal (10,
 3) DEFAULT NULL,
 "灌肠标志" varchar (1) DEFAULT NULL,
 "大便失禁标志" varchar (1) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "大便次数(次/日)" decimal (3,
 0) DEFAULT NULL,
 "卧床标志" varchar (1) DEFAULT NULL,
 "过敏源代码" varchar (5) DEFAULT NULL,
 "过敏源名称" varchar (50) DEFAULT NULL,
 "护士工号" varchar (20) DEFAULT NULL,
 "护士姓名" varchar (50) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "审核人工号" varchar (20) DEFAULT NULL,
 "审核人姓名" varchar (50) DEFAULT NULL,
 "审核时间" timestamp DEFAULT NULL,
 "记录时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "生命体征测量记录流水号" varchar (64) NOT NULL,
 CONSTRAINT "生命体征测量记录"_"医疗机构代码"_"生命体征测量记录流水号"_PK PRIMARY KEY ("医疗机构代码",
 "生命体征测量记录流水号")
);


COMMENT ON TABLE "生育子女情况" IS '妇女生育子女情况，包括子女姓名、性别、出生政策、证件号码、健康状况等';
COMMENT ON COLUMN "生育子女情况"."子女证件号码" IS '子女各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "生育子女情况"."子女证件类型名称" IS '子女身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "生育子女情况"."子女证件类型代码" IS '子女身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "生育子女情况"."生育证号" IS '按照某一特定编码规则赋予生育证的顺序号';
COMMENT ON COLUMN "生育子女情况"."子女出生政策属性名称" IS '出生政策属性在特定编码体系中的名称';
COMMENT ON COLUMN "生育子女情况"."子女出生政策属性代码" IS '出生政策属性在特定编码体系中的代码';
COMMENT ON COLUMN "生育子女情况"."子女性别名称" IS '子女生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "生育子女情况"."子女性别代码" IS '子女生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "生育子女情况"."子女姓名" IS '子女在公安户籍管理部门正式登记注册的姓氏和名称。未取名者填“C”+生母姓名';
COMMENT ON COLUMN "生育子女情况"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "生育子女情况"."证件类型名称" IS '个体身份证件所属类别在特定编码体系中的名称';
COMMENT ON COLUMN "生育子女情况"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "生育子女情况"."妇女姓名" IS '妇女本人在公安户籍管理部门正式登记注册的姓氏和名称。未取名者填“C”+生母姓名';
COMMENT ON COLUMN "生育子女情况"."孩次" IS '生育的第几个孩子的排序号';
COMMENT ON COLUMN "生育子女情况"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "生育子女情况"."生育流水号" IS '按照某一特定编码规则赋予生育子女情况记录的唯一标识号';
COMMENT ON COLUMN "生育子女情况"."全员人口个案标识号" IS '按照某一特定编码规则赋予全员人口个案的唯一标识号';
COMMENT ON COLUMN "生育子女情况"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "生育子女情况"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "生育子女情况"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "生育子女情况"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "生育子女情况"."子女死亡原因" IS '子女死亡原因的详细描述';
COMMENT ON COLUMN "生育子女情况"."子女死亡日期" IS '子女死亡当日的公元纪年日期';
COMMENT ON COLUMN "生育子女情况"."子女当前健康状况名称" IS '子女当前健康状况在特定编码体系中的名称';
COMMENT ON COLUMN "生育子女情况"."子女当前健康状况代码" IS '子女当前健康状况在特定编码体系中的代码';
COMMENT ON COLUMN "生育子女情况"."子女出生健康状况名称" IS '子女出生健康状况的名称，如正常、低体重儿、肉眼可见的出生缺陷';
COMMENT ON COLUMN "生育子女情况"."子女出生健康状况代码" IS '子女出生健康状况(如正常、低体重儿、肉眼可见的出生缺陷等)在特定编码体系中的代码';
COMMENT ON COLUMN "生育子女情况"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "生育子女情况"."子女出生日期" IS '子女出生当日的公元纪年日期';
CREATE TABLE IF NOT EXISTS "生育子女情况" (
"子女证件号码" varchar (32) DEFAULT NULL,
 "子女证件类型名称" varchar (50) DEFAULT NULL,
 "子女证件类型代码" varchar (2) DEFAULT NULL,
 "生育证号" varchar (50) DEFAULT NULL,
 "子女出生政策属性名称" varchar (50) DEFAULT NULL,
 "子女出生政策属性代码" varchar (2) DEFAULT NULL,
 "子女性别名称" varchar (50) DEFAULT NULL,
 "子女性别代码" varchar (2) DEFAULT NULL,
 "子女姓名" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型名称" varchar (50) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "妇女姓名" varchar (50) DEFAULT NULL,
 "孩次" varchar (2) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "生育流水号" varchar (64) NOT NULL,
 "全员人口个案标识号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "子女死亡原因" varchar (200) DEFAULT NULL,
 "子女死亡日期" date DEFAULT NULL,
 "子女当前健康状况名称" varchar (50) DEFAULT NULL,
 "子女当前健康状况代码" varchar (2) DEFAULT NULL,
 "子女出生健康状况名称" varchar (50) DEFAULT NULL,
 "子女出生健康状况代码" varchar (2) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "子女出生日期" date DEFAULT NULL,
 CONSTRAINT "生育子女情况"_"生育流水号"_"全员人口个案标识号"_"医疗机构代码"_PK PRIMARY KEY ("生育流水号",
 "全员人口个案标识号",
 "医疗机构代码")
);


COMMENT ON TABLE "疾病诊断目录" IS '医疗机构内诊断名称，包含院内疾病与平台疾病映射关系';
COMMENT ON COLUMN "疾病诊断目录"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "疾病诊断目录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "疾病诊断目录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "疾病诊断目录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "疾病诊断目录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "疾病诊断目录"."记录状态" IS '标识记录是否正常使用的标志，其中：0：正常；1：停用';
COMMENT ON COLUMN "疾病诊断目录"."疾病分类" IS '标识疾病是普通疾病或非普通疾病类别的代码';
COMMENT ON COLUMN "疾病诊断目录"."诊断分类代码" IS '诊断分类(如西医诊断、中医症候、中医疾病等)在特定编码体系中的代码';
COMMENT ON COLUMN "疾病诊断目录"."医保诊断名称" IS '该诊断在医保编码规则下的名称';
COMMENT ON COLUMN "疾病诊断目录"."医保诊断代码" IS '该诊断在医保编码规则下的代码';
COMMENT ON COLUMN "疾病诊断目录"."平台诊断名称" IS '平台中心诊断名称。与平台中心代码对应的上的必填，参照平台下发的icd10或者中医国标97目录';
COMMENT ON COLUMN "疾病诊断目录"."平台诊断代码" IS '平台中心诊断代码。与平台中心代码对应的上的必填，参照平台下发的icd10或者中医国标97目录';
COMMENT ON COLUMN "疾病诊断目录"."诊断名称" IS '医疗机构内诊断名称';
COMMENT ON COLUMN "疾病诊断目录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "疾病诊断目录"."诊断代码" IS '医疗机构内诊断代码';
CREATE TABLE IF NOT EXISTS "疾病诊断目录" (
"医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "记录状态" varchar (1) DEFAULT NULL,
 "疾病分类" varchar (100) DEFAULT NULL,
 "诊断分类代码" varchar (1) DEFAULT NULL,
 "医保诊断名称" varchar (300) DEFAULT NULL,
 "医保诊断代码" varchar (53) DEFAULT NULL,
 "平台诊断名称" varchar (128) DEFAULT NULL,
 "平台诊断代码" varchar (36) DEFAULT NULL,
 "诊断名称" varchar (128) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "诊断代码" varchar (36) NOT NULL,
 CONSTRAINT "疾病诊断目录"_"医疗机构代码"_"诊断代码"_PK PRIMARY KEY ("医疗机构代码",
 "诊断代码")
);


COMMENT ON TABLE "病危(重)通知书" IS '病情、抢救措施、病危通知内容信息说明';
COMMENT ON COLUMN "病危(重)通知书"."疾病诊断名称" IS '疾病诊断在特定编码体系中的名称';
COMMENT ON COLUMN "病危(重)通知书"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "病危(重)通知书"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "病危(重)通知书"."病危(重)通知书流水号" IS '按照某一特定编码规则赋予病危(重)通知书的唯一标识';
COMMENT ON COLUMN "病危(重)通知书"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "病危(重)通知书"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "病危(重)通知书"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "病危(重)通知书"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "病危(重)通知书"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
COMMENT ON COLUMN "病危(重)通知书"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "病危(重)通知书"."门诊就诊流水号" IS '为门诊就诊时，填写门诊就诊流水号，住院就诊流水号填“-”';
COMMENT ON COLUMN "病危(重)通知书"."知情同意书编号" IS '按照某一特定编码规则赋予知情同意书的唯一标识';
COMMENT ON COLUMN "病危(重)通知书"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "病危(重)通知书"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "病危(重)通知书"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "病危(重)通知书"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "病危(重)通知书"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "病危(重)通知书"."疾病诊断代码" IS '疾病诊断在特定编码体系中的代码';
COMMENT ON COLUMN "病危(重)通知书"."就诊次数" IS '对于门(急)诊患者，此处表示患者门(急)诊的累计次数；对于住院患者，此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "病危(重)通知书"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "病危(重)通知书"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "病危(重)通知书"."性别名称" IS '个体生理性别名称';
COMMENT ON COLUMN "病危(重)通知书"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "病危(重)通知书"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "病危(重)通知书"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "病危(重)通知书"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "病危(重)通知书"."病情概括及主要抢救措施" IS '病危(重)通知书中的病情概括及主要抢救措施的描述，包括神志、生命体征、主要器官功能的描述';
COMMENT ON COLUMN "病危(重)通知书"."病危(重)通知内容" IS '患者病情危、重时，由经治医师或值班 医师向患者 家属告知病 情危重情况的详细描述';
COMMENT ON COLUMN "病危(重)通知书"."病危(重)通知时间" IS '病危(重)通知书下达时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病危(重)通知书"."法定代理人姓名" IS '法定代理人签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "病危(重)通知书"."法定代理人与患者的关系代码" IS '本人与特定对象的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "病危(重)通知书"."法定代理人与患者的关系名称" IS '本人与特定对象的关系类别(如户主、配偶、子女、父母等)名称';
COMMENT ON COLUMN "病危(重)通知书"."法定代理人签名时间" IS '患者或法定代理人签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病危(重)通知书"."医师工号" IS '医师的工号';
COMMENT ON COLUMN "病危(重)通知书"."医师姓名" IS '医师签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "病危(重)通知书"."医师签名时间" IS '医师进行电子签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病危(重)通知书"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "病危(重)通知书"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病危(重)通知书"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
CREATE TABLE IF NOT EXISTS "病危(
重)通知书" ("疾病诊断名称" varchar (512) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "病危(重)通知书流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "门诊就诊流水号" varchar (32) DEFAULT NULL,
 "知情同意书编号" varchar (20) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "病情概括及主要抢救措施" text,
 "病危(重)通知内容" text,
 "病危(重)通知时间" timestamp DEFAULT NULL,
 "法定代理人姓名" varchar (50) DEFAULT NULL,
 "法定代理人与患者的关系代码" varchar (1) DEFAULT NULL,
 "法定代理人与患者的关系名称" varchar (100) DEFAULT NULL,
 "法定代理人签名时间" timestamp DEFAULT NULL,
 "医师工号" varchar (20) DEFAULT NULL,
 "医师姓名" varchar (50) DEFAULT NULL,
 "医师签名时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "病危(重)通知书"_"医疗机构代码"_"病危(重)通知书流水号"_PK PRIMARY KEY ("医疗机构代码",
 "病危(重)通知书流水号")
);


COMMENT ON TABLE "病室工作日志记录" IS '病室工作日志记录，包括入院人数、他科转入人数、他区转入人数、出院人数等统计指标';
COMMENT ON COLUMN "病室工作日志记录"."出院人数合计" IS '记录当日从本病区出院的病人数量';
COMMENT ON COLUMN "病室工作日志记录"."他科转入人数" IS '记录当日从其他科室转入本病区的病人数量';
COMMENT ON COLUMN "病室工作日志记录"."入院人数" IS '记录当日新转入本病区的病人数量';
COMMENT ON COLUMN "病室工作日志记录"."原有人数(0时)" IS '记录当日0时本病区中原有的病人数量';
COMMENT ON COLUMN "病室工作日志记录"."病室工作日志记录日期" IS '记录当日公元纪年日期的完整描述';
COMMENT ON COLUMN "病室工作日志记录"."病区名称" IS '患者当前所在病区在特定编码体系中的名称';
COMMENT ON COLUMN "病室工作日志记录"."就诊科室名称" IS '所在科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "病室工作日志记录"."就诊科室代码" IS '按照机构内编码规则赋予所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "病室工作日志记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "病室工作日志记录"."病室工作日志序号" IS '按照某一特定编码规则赋予病室工作日志记录的顺序号，是病室工作日志记录的唯一标识';
COMMENT ON COLUMN "病室工作日志记录"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "病室工作日志记录"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "病室工作日志记录"."他区转入人数" IS '记录当日从其他病区转入本病区的病人数量';
COMMENT ON COLUMN "病室工作日志记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病室工作日志记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病室工作日志记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "病室工作日志记录"."签名时间" IS '签名完成时的公元纪年和日期的完整描述';
COMMENT ON COLUMN "病室工作日志记录"."护士/医生姓名" IS '护士/医生在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "病室工作日志记录"."陪伴人数" IS '记录当日24时本病区中现有的陪护人员数量';
COMMENT ON COLUMN "病室工作日志记录"."危重病人数(0时)" IS '记录当日24时本病区现有的危重病人数量';
COMMENT ON COLUMN "病室工作日志记录"."现有人数(0时)" IS '记录当日24时本病区中现有的病人数量';
COMMENT ON COLUMN "病室工作日志记录"."转往他区人数" IS '记录当日从本病区转到其他病区的病人数量';
COMMENT ON COLUMN "病室工作日志记录"."转往他科人数" IS '记录当日从本病区转到其他科室的病人数量';
COMMENT ON COLUMN "病室工作日志记录"."24小时内死亡人数" IS '记录当日在本病区24小时内死亡的病人数量';
COMMENT ON COLUMN "病室工作日志记录"."死亡人数" IS '记录当日在本病区死亡的病人数量';
CREATE TABLE IF NOT EXISTS "病室工作日志记录" (
"出院人数合计" decimal (10,
 0) DEFAULT NULL,
 "他科转入人数" decimal (10,
 0) DEFAULT NULL,
 "入院人数" decimal (10,
 0) DEFAULT NULL,
 "原有人数(0时)" decimal (10,
 0) DEFAULT NULL,
 "病室工作日志记录日期" date DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "病室工作日志序号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "他区转入人数" decimal (10,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "护士/医生姓名" varchar (50) DEFAULT NULL,
 "陪伴人数" decimal (10,
 0) DEFAULT NULL,
 "危重病人数(0时)" decimal (10,
 0) DEFAULT NULL,
 "现有人数(0时)" decimal (10,
 0) DEFAULT NULL,
 "转往他区人数" decimal (10,
 0) DEFAULT NULL,
 "转往他科人数" decimal (10,
 0) DEFAULT NULL,
 "24小时内死亡人数" decimal (10,
 0) DEFAULT NULL,
 "死亡人数" decimal (10,
 0) DEFAULT NULL,
 CONSTRAINT "病室工作日志记录"_"病室工作日志序号"_"医疗机构代码"_PK PRIMARY KEY ("病室工作日志序号",
 "医疗机构代码")
);


COMMENT ON TABLE "病案首页手术信息" IS '住院病案首页手术明细信息';
COMMENT ON COLUMN "病案首页手术信息"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病案首页手术信息"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病案首页手术信息"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "病案首页手术信息"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "病案首页手术信息"."手术序号" IS '该手术在一次就诊中的手术排序号，序号1一般指的为主要手术';
COMMENT ON COLUMN "病案首页手术信息"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "病案首页手术信息"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "病案首页手术信息"."首页序号" IS '按照某一特定编码规则赋予病案首页的排序号';
COMMENT ON COLUMN "病案首页手术信息"."手术时间" IS '手术及操作完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "病案首页手术信息"."手术代码" IS '手术及操作在特定编码体系中的唯一标识';
COMMENT ON COLUMN "病案首页手术信息"."手术名称" IS '手术及操作在特定编码体系中的名称';
COMMENT ON COLUMN "病案首页手术信息"."手术级别代码" IS '手术及操作的手术级别在特定编码体系中的代码';
COMMENT ON COLUMN "病案首页手术信息"."手术部位代码" IS '实施手术(操作)的人体部位(如双侧鼻孔、臀部、左臂、右眼等)在特定编码体系中的代码';
COMMENT ON COLUMN "病案首页手术信息"."手术部位名称" IS '实施手术的人体部位在特定编码体系中的名称，如双侧鼻孔、臀部、左臂、右眼等';
COMMENT ON COLUMN "病案首页手术信息"."手术切口类别代码" IS '手术切口类别的分类(如0类切口、I类切口、II类切口、III类切口)在特定编码体系中的代码';
COMMENT ON COLUMN "病案首页手术信息"."手术切口类别名称" IS '手术切口类别的分类(如0类切口、I类切口、II类切口、III类切口)在特定编码体系中的名称';
COMMENT ON COLUMN "病案首页手术信息"."切口愈合等级代码" IS '手术切口愈合类别(如甲、乙、丙)在特定编码体系中的代码';
COMMENT ON COLUMN "病案首页手术信息"."主刀医生姓名" IS '实施手术的主要执行人员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "病案首页手术信息"."手术一助姓名" IS '协助手术者完成手术及操作的第1助手在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "病案首页手术信息"."手术二助姓名" IS '协助手术者完成手术及操作的第2助手在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "病案首页手术信息"."麻醉方式代码" IS '为患者进行手术、操作时使用的麻醉方法在特定编码体系中的代码';
COMMENT ON COLUMN "病案首页手术信息"."麻醉医生姓名" IS '对患者实施麻醉的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "病案首页手术信息"."术前诊断代码" IS '术前诊断在特定编码体系中的代码。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "病案首页手术信息"."术后诊断代码" IS '术后诊断在特定编码体系中的代码。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "病案首页手术信息"."手术科室" IS '手术科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "病案首页手术信息"."再次手术标志" IS '标识需要再次手术的标志';
COMMENT ON COLUMN "病案首页手术信息"."择期/急症手术标志" IS '择期/急症手术的标识，1：择期 2：急症';
COMMENT ON COLUMN "病案首页手术信息"."分院代码" IS '按照某一特定编码规则赋予分院的唯一标识';
COMMENT ON COLUMN "病案首页手术信息"."分院名称" IS '分院的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "病案首页手术信息"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
CREATE TABLE IF NOT EXISTS "病案首页手术信息" (
"数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "手术序号" varchar (2) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "住院就诊流水号" varchar (32) NOT NULL,
 "首页序号" varchar (50) DEFAULT NULL,
 "手术时间" timestamp DEFAULT NULL,
 "手术代码" varchar (20) DEFAULT NULL,
 "手术名称" varchar (200) DEFAULT NULL,
 "手术级别代码" varchar (4) DEFAULT NULL,
 "手术部位代码" varchar (20) DEFAULT NULL,
 "手术部位名称" varchar (50) DEFAULT NULL,
 "手术切口类别代码" varchar (2) DEFAULT NULL,
 "手术切口类别名称" varchar (50) DEFAULT NULL,
 "切口愈合等级代码" varchar (3) DEFAULT NULL,
 "主刀医生姓名" varchar (50) DEFAULT NULL,
 "手术一助姓名" varchar (50) DEFAULT NULL,
 "手术二助姓名" varchar (50) DEFAULT NULL,
 "麻醉方式代码" varchar (4) DEFAULT NULL,
 "麻醉医生姓名" varchar (50) DEFAULT NULL,
 "术前诊断代码" varchar (100) DEFAULT NULL,
 "术后诊断代码" varchar (64) DEFAULT NULL,
 "手术科室" varchar (4) DEFAULT NULL,
 "再次手术标志" varchar (1) DEFAULT NULL,
 "择期/急症手术标志" varchar (1) DEFAULT NULL,
 "分院代码" varchar (2) DEFAULT NULL,
 "分院名称" varchar (120) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 CONSTRAINT "病案首页手术信息"_"医疗机构代码"_"手术序号"_"住院就诊流水号"_PK PRIMARY KEY ("医疗机构代码",
 "手术序号",
 "住院就诊流水号")
);


COMMENT ON TABLE "科室信息" IS '医疗机构科室类型、上级科室、科室特色等基本信息';
COMMENT ON COLUMN "科室信息"."科室名称" IS '医院科室名称';
COMMENT ON COLUMN "科室信息"."医疗机构组织机构代码" IS '经《医疗机构执业许可证》登记的，并按照特定编码体系填写的代码。';
COMMENT ON COLUMN "科室信息"."平台科室代码" IS '该科室对应平台标准的代码';
COMMENT ON COLUMN "科室信息"."社保局代码" IS '该科室对应社保局标准的科室编号';
COMMENT ON COLUMN "科室信息"."说明" IS '科室的创建、组成、发展、特色等信息说明';
COMMENT ON COLUMN "科室信息"."门诊类型代码" IS '门诊类型(如普通、急诊、专家、特需等)在特定编码体系中的代码';
COMMENT ON COLUMN "科室信息"."中医科室标志" IS '是否中医科室的标志';
COMMENT ON COLUMN "科室信息"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "科室信息"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "科室信息"."科室代码" IS '医院科室代码';
COMMENT ON COLUMN "科室信息"."可挂号标志" IS '标识该科室是否可以挂号的标志';
COMMENT ON COLUMN "科室信息"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "科室信息"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "科室信息"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "科室信息"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
CREATE TABLE IF NOT EXISTS "科室信息" (
"科室名称" varchar (100) DEFAULT NULL,
 "医疗机构组织机构代码" varchar (50) DEFAULT NULL,
 "平台科室代码" varchar (20) DEFAULT NULL,
 "社保局代码" varchar (32) DEFAULT NULL,
 "说明" varchar (500) DEFAULT NULL,
 "门诊类型代码" varchar (1) DEFAULT NULL,
 "中医科室标志" varchar (1) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "科室代码" varchar (20) NOT NULL,
 "可挂号标志" varchar (1) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 CONSTRAINT "科室信息"_"医疗机构代码"_"科室代码"_PK PRIMARY KEY ("医疗机构代码",
 "科室代码")
);


COMMENT ON TABLE "药品供应商信息表" IS '药品供应商的基本信息，包括厂家名称、统一社会信息代码、银行账号、厂家类型等';
COMMENT ON COLUMN "药品供应商信息表"."开户银行" IS '与药品供应商建立往来帐户代理其业务并提供服务的银行名称。';
COMMENT ON COLUMN "药品供应商信息表"."银行账号" IS '与药品供应商建立往来帐户代理其业务并提供服务的银行的开户账号。';
COMMENT ON COLUMN "药品供应商信息表"."联系电话号码" IS '药品供应商所属公司的企业电话号码';
COMMENT ON COLUMN "药品供应商信息表"."邮箱地址" IS '药品供应商的电子邮箱名称';
COMMENT ON COLUMN "药品供应商信息表"."供应商类型代码" IS '供应商类型在特定编码体系中的代码';
COMMENT ON COLUMN "药品供应商信息表"."备注" IS '其他重要信息的补充说明';
COMMENT ON COLUMN "药品供应商信息表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "药品供应商信息表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "药品供应商信息表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "药品供应商信息表"."联系人姓名" IS '药品供应商联系人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "药品供应商信息表"."邮政编码" IS '药品供应商所属公司的企业邮政编码';
COMMENT ON COLUMN "药品供应商信息表"."地址" IS '药品供应商注册地址的详细描述';
COMMENT ON COLUMN "药品供应商信息表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "药品供应商信息表"."医疗机构名称" IS '医疗服务机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "药品供应商信息表"."供应商代码" IS '按照某一特定编码规则赋予药品供应商的唯一标识';
COMMENT ON COLUMN "药品供应商信息表"."修改标志" IS '数据上传的标志';
COMMENT ON COLUMN "药品供应商信息表"."供应商名称" IS '药品供应商在工商局注册、审批通过后的企业名称';
COMMENT ON COLUMN "药品供应商信息表"."供应商简称" IS '药品供应商名称的简称或别名';
COMMENT ON COLUMN "药品供应商信息表"."组织机构代码" IS '药品供应商的组织机构代码';
CREATE TABLE IF NOT EXISTS "药品供应商信息表" (
"开户银行" varchar (64) DEFAULT NULL,
 "银行账号" varchar (32) DEFAULT NULL,
 "联系电话号码" varchar (20) DEFAULT NULL,
 "邮箱地址" varchar (32) DEFAULT NULL,
 "供应商类型代码" varchar (1) DEFAULT NULL,
 "备注" varchar (1000) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "联系人姓名" varchar (50) DEFAULT NULL,
 "邮政编码" varchar (6) DEFAULT NULL,
 "地址" varchar (128) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "供应商代码" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "供应商名称" varchar (128) DEFAULT NULL,
 "供应商简称" varchar (100) DEFAULT NULL,
 "组织机构代码" varchar (32) DEFAULT NULL,
 CONSTRAINT "药品供应商信息表"_"医疗机构代码"_"供应商代码"_PK PRIMARY KEY ("医疗机构代码",
 "供应商代码")
);


COMMENT ON TABLE "血液库存信息表" IS '血液库存信息，包括血液名称、库存量，库存上下限等信息';
COMMENT ON COLUMN "血液库存信息表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "血液库存信息表"."库存流水号" IS '按照某一特定编码规则赋予本次库存记录流水号唯一标志的顺序号';
COMMENT ON COLUMN "血液库存信息表"."库存上限" IS '对应血量计量单位的库存数量的上限值';
COMMENT ON COLUMN "血液库存信息表"."血液库存" IS '对应血量计量单位的库存数量';
COMMENT ON COLUMN "血液库存信息表"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "血液库存信息表"."血液品种代码" IS '全血或血液成分类别(如红细胞、全血、血小板、血浆等)在特定编码体系中的代码';
COMMENT ON COLUMN "血液库存信息表"."Rh血型代码" IS '为患者实际输入的Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "血液库存信息表"."ABO血型代码" IS '为患者实际输入的ABO血型类别在特定编码体系中的代码';
COMMENT ON COLUMN "血液库存信息表"."医疗机构代码" IS '医疗机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "血液库存信息表"."血液产品名称代码" IS '血液产品名称代码，如去白全血|ACD-B/200ml01200.001.00ACD-B、全血|CPDA-1010.000.00CPDA-1等';
COMMENT ON COLUMN "血液库存信息表"."血液产品大类代码" IS '血液产品类别(如全血、红细胞、新鲜冰冻血浆、汇集血小板等)在特定编码体系中的代码';
COMMENT ON COLUMN "血液库存信息表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血液库存信息表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血液库存信息表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "血液库存信息表"."血量计量单位" IS '血液或血液成分的计量单位的机构内名称，如治疗量、U、ML等';
COMMENT ON COLUMN "血液库存信息表"."库存下限" IS '对应血量计量单位的库存数量的下限值';
CREATE TABLE IF NOT EXISTS "血液库存信息表" (
"修改标志" varchar (1) DEFAULT NULL,
 "库存流水号" varchar (36) NOT NULL,
 "库存上限" varchar (20) DEFAULT NULL,
 "血液库存" varchar (10) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "血液品种代码" varchar (5) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "血液产品名称代码" varchar (36) DEFAULT NULL,
 "血液产品大类代码" varchar (36) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "血量计量单位" varchar (10) DEFAULT NULL,
 "库存下限" varchar (20) DEFAULT NULL,
 CONSTRAINT "血液库存信息表"_"库存流水号"_"医疗机构代码"_PK PRIMARY KEY ("库存流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "血透治疗医嘱明细" IS '患者血液透析治疗相关的医嘱明细信息';
COMMENT ON COLUMN "血透治疗医嘱明细"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血透治疗医嘱明细"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "血透治疗医嘱明细"."护士姓名" IS '执行护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "血透治疗医嘱明细"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "血透治疗医嘱明细"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "血透治疗医嘱明细"."医生姓名" IS '医嘱开立医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "血透治疗医嘱明细"."执行时间" IS '医嘱执行完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血透治疗医嘱明细"."药品用法" IS '对治疗疾病所用药物的具体服用方法的详细描述';
COMMENT ON COLUMN "血透治疗医嘱明细"."药品用量" IS '单次使用药物的剂量，按剂量单位计';
COMMENT ON COLUMN "血透治疗医嘱明细"."医嘱内容" IS '医嘱内容的详细描述，药品医嘱直接填报药品名称';
COMMENT ON COLUMN "血透治疗医嘱明细"."医嘱开立时间" IS '医嘱开立时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血透治疗医嘱明细"."药品医嘱标志" IS '标识医嘱是否为药品类医嘱的标志';
COMMENT ON COLUMN "血透治疗医嘱明细"."透析编号" IS '按照一定编码规则赋予本次透析治疗的唯一标识';
COMMENT ON COLUMN "血透治疗医嘱明细"."医嘱明细流水号" IS '按照某一特定编码规则赋予每条医嘱明细的唯一标识';
COMMENT ON COLUMN "血透治疗医嘱明细"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "血透治疗医嘱明细"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
CREATE TABLE IF NOT EXISTS "血透治疗医嘱明细" (
"数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "护士姓名" varchar (50) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "医生姓名" varchar (50) DEFAULT NULL,
 "执行时间" timestamp DEFAULT NULL,
 "药品用法" varchar (32) DEFAULT NULL,
 "药品用量" varchar (50) DEFAULT NULL,
 "医嘱内容" varchar (100) DEFAULT NULL,
 "医嘱开立时间" timestamp DEFAULT NULL,
 "药品医嘱标志" varchar (1) DEFAULT NULL,
 "透析编号" varchar (64) DEFAULT NULL,
 "医嘱明细流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 CONSTRAINT "血透治疗医嘱明细"_"医疗机构代码"_"医嘱明细流水号"_PK PRIMARY KEY ("医疗机构代码",
 "医嘱明细流水号")
);


COMMENT ON TABLE "血透治疗过程记录" IS '患者血液透析治疗过程中的各项指标监测结果数据';
COMMENT ON COLUMN "血透治疗过程记录"."超滤量" IS '患者本次透析治疗过程中的超滤量，计量单位ml';
COMMENT ON COLUMN "血透治疗过程记录"."TMP" IS 'TMP跨膜压的测量值，计量单位mmhg';
COMMENT ON COLUMN "血透治疗过程记录"."静脉压" IS '静脉压的测量值，计量单位mmhg';
COMMENT ON COLUMN "血透治疗过程记录"."血流量" IS '实时血流量，计量单位ml';
COMMENT ON COLUMN "血透治疗过程记录"."心率(次/min)" IS '心脏搏动频率的测量值,计量单位为次/min';
COMMENT ON COLUMN "血透治疗过程记录"."透析液温度" IS '透析液温度，计量单位℃';
COMMENT ON COLUMN "血透治疗过程记录"."舒张压(mmHg)" IS '舒张压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "血透治疗过程记录"."收缩压(mmHg)" IS '收缩压的测量值，计量单位为mmHg';
COMMENT ON COLUMN "血透治疗过程记录"."监测时间" IS '指标监测完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血透治疗过程记录"."透析编号" IS '按照一定编码规则赋予本次透析治疗的唯一标识';
COMMENT ON COLUMN "血透治疗过程记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "血透治疗过程记录"."监测记录流水号" IS '按照一定编码规则赋予监测记录的唯一标识';
COMMENT ON COLUMN "血透治疗过程记录"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "血透治疗过程记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "血透治疗过程记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血透治疗过程记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "血透治疗过程记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
CREATE TABLE IF NOT EXISTS "血透治疗过程记录" (
"超滤量" decimal (8,
 0) DEFAULT NULL,
 "TMP" decimal (4,
 0) DEFAULT NULL,
 "静脉压" decimal (4,
 0) DEFAULT NULL,
 "血流量" decimal (4,
 0) DEFAULT NULL,
 "心率(次/min)" decimal (4,
 1) DEFAULT NULL,
 "透析液温度" decimal (4,
 1) DEFAULT NULL,
 "舒张压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "收缩压(mmHg)" decimal (3,
 0) DEFAULT NULL,
 "监测时间" timestamp DEFAULT NULL,
 "透析编号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "监测记录流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 CONSTRAINT "血透治疗过程记录"_"监测记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("监测记录流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "诊断明细报告" IS '本次就诊过程中相关的诊断信息';
COMMENT ON COLUMN "诊断明细报告"."诊断分类代码" IS '诊断分类(如西医诊断、中医症候、中医疾病等)在特定编码体系中的代码';
COMMENT ON COLUMN "诊断明细报告"."入院诊断顺位" IS '表示诊断的顺位及其从属关系的标志';
COMMENT ON COLUMN "诊断明细报告"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "诊断明细报告"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "诊断明细报告"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "诊断明细报告"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "诊断明细报告"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "诊断明细报告"."病理号" IS '病理诊断对应的病理号';
COMMENT ON COLUMN "诊断明细报告"."住院次数" IS '患者住院次数统计';
COMMENT ON COLUMN "诊断明细报告"."就诊事件类型名称" IS '就诊类型在特定编码体系中的名称';
COMMENT ON COLUMN "诊断明细报告"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
COMMENT ON COLUMN "诊断明细报告"."就诊流水号" IS '按照某一特定编码规则赋予就诊事件的唯一标识';
COMMENT ON COLUMN "诊断明细报告"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "诊断明细报告"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "诊断明细报告"."诊断流水号" IS '按照特定编码规则赋予某一条诊断明细记录唯一标识的顺序号';
COMMENT ON COLUMN "诊断明细报告"."诊断说明" IS '关于诊断的详细说明';
COMMENT ON COLUMN "诊断明细报告"."病情转归代码" IS '患者所患的每种疾病的治疗结果类别(如治愈、好转、稳定、恶化等)在特定编码体系中的代码';
COMMENT ON COLUMN "诊断明细报告"."入院病情代码" IS '入院病情评估情况(如临床未确定、情况不明等)在特定编码体系中的代码';
COMMENT ON COLUMN "诊断明细报告"."诊断状态代码" IS '疾病的诊断状态类型(如疑似病例、临床诊断病例、实验室确诊病例、病原携带者等)在特定编码体系中的代码';
COMMENT ON COLUMN "诊断明细报告"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "诊断明细报告"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "诊断明细报告"."诊断依据名称" IS '患者诊断依据在特定编码体系中的名称';
COMMENT ON COLUMN "诊断明细报告"."诊断依据代码" IS '患者诊断依据在特定编码体系中的代码';
COMMENT ON COLUMN "诊断明细报告"."中医症候名称" IS '患者所患疾病在中医证候特定分类体系中的名称';
COMMENT ON COLUMN "诊断明细报告"."中医症候代码" IS '患者所患疾病在中医证候特定分类体系中的代码';
COMMENT ON COLUMN "诊断明细报告"."中医病名名称" IS '患者所患疾病在中医病名特定分类体系中的名称';
COMMENT ON COLUMN "诊断明细报告"."中医病名代码" IS '患者所患疾病在中医病名特定分类体系中的代码';
COMMENT ON COLUMN "诊断明细报告"."西医诊断名称" IS '患者所患疾病在西医诊断特定编码体系中的名称';
COMMENT ON COLUMN "诊断明细报告"."西医诊断代码" IS '患者所患疾病在西医诊断特定编码体系中的编码';
COMMENT ON COLUMN "诊断明细报告"."病案归档时间" IS '病案归档时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "诊断明细报告"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "诊断明细报告"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "诊断明细报告"."疾病诊断代码类型" IS '疾病诊断编码类别(如ICD10、中医国标-95、中医国标-97等)在特定编码体系中的代码';
COMMENT ON COLUMN "诊断明细报告"."主要诊断标志" IS '标识疾病诊断是主要诊断的标志';
COMMENT ON COLUMN "诊断明细报告"."诊断时间" IS '对患者罹患疾病做出诊断时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "诊断明细报告"."诊断类别名称" IS '诊断类别在特定编码体系中的名称，如门急诊诊断、出院诊断、病理诊断、损伤中毒诊断等';
COMMENT ON COLUMN "诊断明细报告"."诊断类别代码" IS '诊断类别(如门急诊诊断、出院诊断、病理诊断、损伤中毒诊断等)在特定编码体系中的代码';
CREATE TABLE IF NOT EXISTS "诊断明细报告" (
"诊断分类代码" varchar (1) DEFAULT NULL,
 "入院诊断顺位" varchar (20) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "病理号" varchar (20) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "就诊事件类型名称" varchar (10) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "就诊流水号" varchar (32) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "诊断流水号" varchar (32) NOT NULL,
 "诊断说明" varchar (512) DEFAULT NULL,
 "病情转归代码" varchar (3) DEFAULT NULL,
 "入院病情代码" varchar (3) DEFAULT NULL,
 "诊断状态代码" varchar (3) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "诊断依据名称" varchar (100) DEFAULT NULL,
 "诊断依据代码" varchar (30) DEFAULT NULL,
 "中医症候名称" varchar (50) DEFAULT NULL,
 "中医症候代码" varchar (50) DEFAULT NULL,
 "中医病名名称" varchar (50) DEFAULT NULL,
 "中医病名代码" varchar (50) DEFAULT NULL,
 "西医诊断名称" varchar (100) DEFAULT NULL,
 "西医诊断代码" varchar (20) DEFAULT NULL,
 "病案归档时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "疾病诊断代码类型" varchar (2) DEFAULT NULL,
 "主要诊断标志" varchar (1) DEFAULT NULL,
 "诊断时间" timestamp DEFAULT NULL,
 "诊断类别名称" varchar (200) DEFAULT NULL,
 "诊断类别代码" varchar (2) DEFAULT NULL,
 CONSTRAINT "诊断明细报告"_"诊断流水号"_"医疗机构代码"_PK PRIMARY KEY ("诊断流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "转诊记录_检验检查" IS '患者转院时关于患者各种检查检验的记录';
COMMENT ON COLUMN "转诊记录_检验检查"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "转诊记录_检验检查"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "转诊记录_检验检查"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "转诊记录_检验检查"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "转诊记录_检验检查"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "转诊记录_检验检查"."检验检查记录流水号" IS '按照一定编码规则赋予检验检查记录的唯一标识';
COMMENT ON COLUMN "转诊记录_检验检查"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "转诊记录_检验检查"."转诊记录流水号" IS '按照某一特性编码规则赋予转诊(转院)记录的唯一标识';
COMMENT ON COLUMN "转诊记录_检验检查"."检查/检验类别代码" IS '受检者检查/检验项目所属的类别(如问询、物理、实验室、影像)在特定编码体系中的代码';
COMMENT ON COLUMN "转诊记录_检验检查"."检查/检验类别名称" IS '受检者检查/检验项目所属的类别(如问询、物理、实验室、影像)在特定编码体系中的名称';
COMMENT ON COLUMN "转诊记录_检验检查"."检查/检验项目代码" IS '检查/检验指标项目在特定编码体系中的代码';
COMMENT ON COLUMN "转诊记录_检验检查"."检查/检验项目名称" IS '检查/检验指标项目(红细胞计数、血红蛋白、白细胞计数、真菌药敏实验、衣原体培养及药敏等)在特定编码体系中的名称';
COMMENT ON COLUMN "转诊记录_检验检查"."检查/检验结果代码" IS '受检者检查/检验结果(如正常、异常、不详等)在特定编码体系中的代码';
COMMENT ON COLUMN "转诊记录_检验检查"."检查/检验结果名称" IS '受检者检查/检验结果(如正常、异常、不详等)在特定编码体系中的名称';
COMMENT ON COLUMN "转诊记录_检验检查"."检查/检验定量结果" IS '受检者检查/检验细项的定量结果的详细描述，如5.6或<0.35等';
COMMENT ON COLUMN "转诊记录_检验检查"."检查/检验计量单位" IS '检验/检查定量结果计量单位的名称';
CREATE TABLE IF NOT EXISTS "转诊记录_检验检查" (
"密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "检验检查记录流水号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "转诊记录流水号" varchar (64) DEFAULT NULL,
 "检查/检验类别代码" varchar (1) DEFAULT NULL,
 "检查/检验类别名称" varchar (50) DEFAULT NULL,
 "检查/检验项目代码" varchar (32) DEFAULT NULL,
 "检查/检验项目名称" varchar (80) DEFAULT NULL,
 "检查/检验结果代码" varchar (4) DEFAULT NULL,
 "检查/检验结果名称" varchar (20) DEFAULT NULL,
 "检查/检验定量结果" decimal (10,
 0) DEFAULT NULL,
 "检查/检验计量单位" varchar (20) DEFAULT NULL,
 CONSTRAINT "转诊记录_检验检查"_"医疗机构代码"_"检验检查记录流水号"_PK PRIMARY KEY ("医疗机构代码",
 "检验检查记录流水号")
);


COMMENT ON TABLE "输液记录" IS '患者输液过程中相关人员、时间等的记录';
COMMENT ON COLUMN "输液记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "输液记录"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "输液记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "输液记录"."输液执行人工号" IS '输液执行员在原始特定编码体系中的编号。';
COMMENT ON COLUMN "输液记录"."输液执行人姓名" IS '输液执行员在公安管理部门正式配液注册的姓氏和名称';
COMMENT ON COLUMN "输液记录"."输液开始时间" IS '输液开始发生时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输液记录"."输液结束时间" IS '输液停止时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输液记录"."接瓶记录时间" IS '为患者接瓶时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输液记录"."接瓶情况记录" IS '为患者接瓶时的情况描述';
COMMENT ON COLUMN "输液记录"."异常记录时间" IS '患者输液发生异常时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输液记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "输液记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输液记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输液记录"."配液时间" IS '配液完成当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输液记录"."配液人姓名" IS '配液员在公安管理部门正式配液注册的姓氏和名称';
COMMENT ON COLUMN "输液记录"."配液人工号" IS '配液员在原始特定编码体系中的编号。';
COMMENT ON COLUMN "输液记录"."输液明细流水号" IS '按照某一特定编码规则赋予输液明细的唯一性编号';
COMMENT ON COLUMN "输液记录"."登记时间" IS '完成登记时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输液记录"."输液登记人姓名" IS '输液登记员在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "输液记录"."输液登记人工号" IS '输液登记员在原始特定编码体系中的编号';
COMMENT ON COLUMN "输液记录"."处方药品组号" IS '同一组相同治疗目的医嘱/处方的顺序号';
COMMENT ON COLUMN "输液记录"."处方编号" IS '按照某一特定编码规则赋予医嘱/处方单的唯一性编号';
COMMENT ON COLUMN "输液记录"."输液流水序号" IS '按照某一特定编码规则赋予输液流水的唯一性编号';
COMMENT ON COLUMN "输液记录"."医嘱单号" IS '按照某一特定编码规则赋予医嘱本地的唯一性编号';
COMMENT ON COLUMN "输液记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "输液记录"."就诊科室名称" IS '就诊科室的机构内名称';
COMMENT ON COLUMN "输液记录"."就诊科室代码" IS '按照机构内编码规则赋予就诊科室的唯一标识';
CREATE TABLE IF NOT EXISTS "输液记录" (
"修改标志" varchar (1) DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "输液执行人工号" varchar (20) DEFAULT NULL,
 "输液执行人姓名" varchar (50) DEFAULT NULL,
 "输液开始时间" timestamp DEFAULT NULL,
 "输液结束时间" timestamp DEFAULT NULL,
 "接瓶记录时间" timestamp DEFAULT NULL,
 "接瓶情况记录" varchar (200) DEFAULT NULL,
 "异常记录时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "配液时间" timestamp DEFAULT NULL,
 "配液人姓名" varchar (50) DEFAULT NULL,
 "配液人工号" varchar (20) DEFAULT NULL,
 "输液明细流水号" varchar (60) NOT NULL,
 "登记时间" timestamp DEFAULT NULL,
 "输液登记人姓名" varchar (50) DEFAULT NULL,
 "输液登记人工号" varchar (20) DEFAULT NULL,
 "处方药品组号" varchar (32) DEFAULT NULL,
 "处方编号" varchar (32) DEFAULT NULL,
 "输液流水序号" varchar (60) DEFAULT NULL,
 "医嘱单号" varchar (32) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 CONSTRAINT "输液记录"_"医疗机构代码"_"输液明细流水号"_PK PRIMARY KEY ("医疗机构代码",
 "输液明细流水号")
);


COMMENT ON TABLE "输血申请明细表" IS '输血申请 各血液品种明细的信息';
COMMENT ON COLUMN "输血申请明细表"."血量单位代码" IS '血液或血液成分的计量单位(如治疗量、U、ML等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血申请明细表"."血量单位名称" IS '血液或血液成分的计量单位的标准名称，如治疗量、U、ML等';
COMMENT ON COLUMN "输血申请明细表"."输血目的" IS '本次输血的目的描述';
COMMENT ON COLUMN "输血申请明细表"."记录时间" IS '完成记录时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血申请明细表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "输血申请明细表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血申请明细表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血申请明细表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "输血申请明细表"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "输血申请明细表"."输血明细流水号" IS '按照某一特定编码规则赋予输血申请明细的唯一标识';
COMMENT ON COLUMN "输血申请明细表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "输血申请明细表"."申请流水号" IS '按照某一特定编码规则赋予输血申请单的唯一标识';
COMMENT ON COLUMN "输血申请明细表"."血液品种代码" IS '申请的全血或血液成分类别(如红细胞、全血、血小板、血浆等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血申请明细表"."血量" IS '申请输入红细胞、血小板、血浆、全血等的数量';
CREATE TABLE IF NOT EXISTS "输血申请明细表" (
"血量单位代码" varchar (1) DEFAULT NULL,
 "血量单位名称" varchar (50) DEFAULT NULL,
 "输血目的" varchar (50) DEFAULT NULL,
 "记录时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "输血明细流水号" varchar (36) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "申请流水号" varchar (36) DEFAULT NULL,
 "血液品种代码" varchar (5) DEFAULT NULL,
 "血量" varchar (50) DEFAULT NULL,
 CONSTRAINT "输血申请明细表"_"医疗机构代码"_"输血明细流水号"_PK PRIMARY KEY ("医疗机构代码",
 "输血明细流水号")
);


COMMENT ON TABLE "输血记录表" IS '患者输血品种的明细信息，包括输血史、输血时间、输血量以及各项指标结果';
COMMENT ON COLUMN "输血记录表"."记录时间" IS '完成记录时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血记录表"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "输血记录表"."记录流水号" IS '按照某一特定编码规则赋予输血记录的唯一标识';
COMMENT ON COLUMN "输血记录表"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "输血记录表"."就诊流水号" IS '按照某一特定编码规则赋予特定业务事件的唯一标识，如门急诊就诊流水号、住院就诊流水号。对门诊类型，一般可采用挂号时HIS产生的就诊流水号；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "输血记录表"."申请流水号" IS '按照某一特定编码规则赋予输血申请单的唯一标识';
COMMENT ON COLUMN "输血记录表"."血袋编号" IS '全省唯一血袋编码，来源于血站';
COMMENT ON COLUMN "输血记录表"."就诊事件类型代码" IS '就诊事件类型如门诊、住院等在特定编码体系中的代码';
COMMENT ON COLUMN "输血记录表"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "输血记录表"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血记录表"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "输血记录表"."证件类型代码" IS '个体身份证件所属类别在特定编码体系中的代码';
COMMENT ON COLUMN "输血记录表"."身份证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "输血记录表"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "输血记录表"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "输血记录表"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "输血记录表"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "输血记录表"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "输血记录表"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "输血记录表"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "输血记录表"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "输血记录表"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "输血记录表"."RH血型代码" IS '为患者实际输入的Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "输血记录表"."RH血型名称" IS '为患者实际输入的Rh血型的类别在特定编码体系中的名称';
COMMENT ON COLUMN "输血记录表"."ABO血型代码" IS '为患者实际输入的ABO血型类别在特定编码体系中的代码';
COMMENT ON COLUMN "输血记录表"."ABO血型名称" IS '为患者实际输入的ABO血型类别在特定编码体系中的名称';
COMMENT ON COLUMN "输血记录表"."输血史标志" IS '标识既往是否有输血经历的的标志';
COMMENT ON COLUMN "输血记录表"."诊断分类代码" IS '诊断分类(如西医诊断、中医症候、中医疾病等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血记录表"."疾病诊断代码" IS '疾病诊断在特定编码体系中的代码。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "输血记录表"."疾病诊断名称" IS '疾病诊断在特定编码体系中的名称。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "输血记录表"."诊断时间" IS '对患者罹患疾病做出诊断时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血记录表"."申请单号" IS '按照某一特定编码规则赋予电子申请单的顺序号';
COMMENT ON COLUMN "输血记录表"."输血性质代码" IS '输血性质类别(备血、常规、紧急等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血记录表"."输血性质名称" IS '输血性质类别在特定编码体系中的名称，如备血、常规、紧急等';
COMMENT ON COLUMN "输血记录表"."申请ABO血型代码" IS '为患者申请的ABO血型类别在特定编码体系中的代码';
COMMENT ON COLUMN "输血记录表"."申请ABO血型名称" IS '为患者申请的ABO血型类别在特定编码体系中的名称';
COMMENT ON COLUMN "输血记录表"."申请Rh血型代码" IS '为患者申请的Rh血型类别在特定编码体系中的代码';
COMMENT ON COLUMN "输血记录表"."申请Rh血型名称" IS '为患者申请的Rh血型类别在特定编码体系中的名称';
COMMENT ON COLUMN "输血记录表"."输血品种代码" IS '输入全血或血液成分类别(如红细胞、全血、血小板、血浆等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血记录表"."输血品种名称" IS '输入全血或血液成分类别在特定编码体系中的名称，如红细胞、全血、血小板、血浆等';
COMMENT ON COLUMN "输血记录表"."输血量(mL)" IS '输入红细胞、血小板、血浆、全血等的数量，剂量单位为mL';
COMMENT ON COLUMN "输血记录表"."输血量计量单位" IS '输入血液或血液成分的计量单位，可包含汉字的字符，如mL，单位，治疗量等';
COMMENT ON COLUMN "输血记录表"."交叉配血结果" IS '交叉配血结果的代码，其中：1：有凝集  2：无凝集';
COMMENT ON COLUMN "输血记录表"."输血开始时间" IS '输血开始时间的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血记录表"."输血结束时间" IS '输血结束时间的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血记录表"."执行护士工号" IS '执行护士在机构内编码体系中的编号';
COMMENT ON COLUMN "输血记录表"."执行护士姓名" IS '执行护士在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "输血记录表"."输血前体温" IS '输血前体温的测量值，计量单位为℃';
COMMENT ON COLUMN "输血记录表"."输血前心率" IS '输血前心脏搏动频率的测量值,计量单位为次/min';
COMMENT ON COLUMN "输血记录表"."输血前呼吸频率" IS '输血前单位时间内呼吸的次数,计壁单位为次/min';
COMMENT ON COLUMN "输血记录表"."输血前血压" IS '输血前受检者收缩压和舒张压的测量值,计量单位为mmHg';
COMMENT ON COLUMN "输血记录表"."输血前指标检测人员姓名" IS '输血前指标检测人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "输血记录表"."输血后体温" IS '输血后体温的测量值，计量单位为℃';
COMMENT ON COLUMN "输血记录表"."输血后心率" IS '输血后心脏搏动频率的测量值,计量单位为次/min';
COMMENT ON COLUMN "输血记录表"."输血后呼吸频率" IS '输血后单位时间内呼吸的次数,计壁单位为次/min';
COMMENT ON COLUMN "输血记录表"."输血后血压" IS '输血后受检者收缩压和舒张压的测量值,计量单位为mmHg';
COMMENT ON COLUMN "输血记录表"."输血后指标检测人员姓名" IS '输血后指标检测人员在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "输血记录表"."输血前血红蛋白" IS '受检者输血前单位容积血液中血红蛋白的检测数值';
COMMENT ON COLUMN "输血记录表"."输血前红细胞比容" IS '受检者输血前红细胞比容的检测数值';
COMMENT ON COLUMN "输血记录表"."输血前红细胞数" IS '受检者输血前单位容积血液内红细胞的数量值';
COMMENT ON COLUMN "输血记录表"."输血前白细胞数" IS '受检者输血前单位容积血液中白细胞数量值';
COMMENT ON COLUMN "输血记录表"."输血前血小板值" IS '受检者输血前单位容积血液内血小板的数量值';
COMMENT ON COLUMN "输血记录表"."输血前凝血酶原时间" IS '受检者输血前凝血酶原时间的检测数值';
COMMENT ON COLUMN "输血记录表"."输血前活化部分凝血活酶时间" IS '受检者输血前活化部分凝血活酶时间的检测数值';
COMMENT ON COLUMN "输血记录表"."输血前纤维蛋白原" IS '受检者输血前纤维蛋白原的检测数值';
COMMENT ON COLUMN "输血记录表"."输血后血红蛋白" IS '受检者输血后单位容积血液中血红蛋白的检测数值';
COMMENT ON COLUMN "输血记录表"."输血后红细胞比容" IS '受检者输血后红细胞比容的检测数值';
COMMENT ON COLUMN "输血记录表"."输血后红细胞数" IS '受检者输血后单位容积血液内红细胞的数量值';
COMMENT ON COLUMN "输血记录表"."输血后白细胞数" IS '受检者输血后单位容积血液中白细胞数量值';
COMMENT ON COLUMN "输血记录表"."输血后血小板值" IS '受检者输血后单位容积血液内血小板的数量值';
COMMENT ON COLUMN "输血记录表"."输血后凝血酶原时间" IS '受检者输血后凝血酶原时间的检测数值';
COMMENT ON COLUMN "输血记录表"."输血后活化部分凝血活酶时间" IS '受检者输血后活化部分凝血活酶时间的检测数值';
COMMENT ON COLUMN "输血记录表"."输血后纤维蛋白原" IS '受检者输血后纤维蛋白原的检测数值';
COMMENT ON COLUMN "输血记录表"."输血反应标志" IS '标志患者术中输血后是否发生了输血反应的标志';
COMMENT ON COLUMN "输血记录表"."输血反应类型代码" IS '患者发生输血反应的分类(如发热、过敏、溶血等)在特定编码体系中的代码';
COMMENT ON COLUMN "输血记录表"."输血反应类型名称" IS '患者发生输血反应的分类在标准体系中的名称，如发热、过敏、溶血等';
COMMENT ON COLUMN "输血记录表"."不良反应描述" IS '输血不良反应情况的详细描述';
COMMENT ON COLUMN "输血记录表"."不良反应处理方法" IS '不良反应处理方法的详细描述';
COMMENT ON COLUMN "输血记录表"."输血原因" IS '表示患者本次输血的原因描述';
COMMENT ON COLUMN "输血记录表"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "输血记录表"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血记录表"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "输血记录表"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
CREATE TABLE IF NOT EXISTS "输血记录表" (
"记录时间" timestamp DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "记录流水号" varchar (36) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "就诊流水号" varchar (32) DEFAULT NULL,
 "申请流水号" varchar (50) DEFAULT NULL,
 "血袋编号" varchar (50) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "身份证件号码" varchar (32) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "RH血型代码" varchar (1) DEFAULT NULL,
 "RH血型名称" varchar (50) DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "ABO血型名称" varchar (50) DEFAULT NULL,
 "输血史标志" varchar (1) DEFAULT NULL,
 "诊断分类代码" varchar (1) DEFAULT NULL,
 "疾病诊断代码" varchar (64) DEFAULT NULL,
 "疾病诊断名称" varchar (512) DEFAULT NULL,
 "诊断时间" timestamp DEFAULT NULL,
 "申请单号" varchar (32) DEFAULT NULL,
 "输血性质代码" varchar (1) DEFAULT NULL,
 "输血性质名称" varchar (50) DEFAULT NULL,
 "申请ABO血型代码" varchar (1) DEFAULT NULL,
 "申请ABO血型名称" varchar (50) DEFAULT NULL,
 "申请Rh血型代码" varchar (1) DEFAULT NULL,
 "申请Rh血型名称" varchar (50) DEFAULT NULL,
 "输血品种代码" varchar (10) DEFAULT NULL,
 "输血品种名称" varchar (50) DEFAULT NULL,
 "输血量(mL)" decimal (4,
 0) DEFAULT NULL,
 "输血量计量单位" varchar (10) DEFAULT NULL,
 "交叉配血结果" varchar (1) DEFAULT NULL,
 "输血开始时间" timestamp DEFAULT NULL,
 "输血结束时间" timestamp DEFAULT NULL,
 "执行护士工号" varchar (20) DEFAULT NULL,
 "执行护士姓名" varchar (50) DEFAULT NULL,
 "输血前体温" decimal (5,
 0) DEFAULT NULL,
 "输血前心率" decimal (3,
 0) DEFAULT NULL,
 "输血前呼吸频率" decimal (3,
 0) DEFAULT NULL,
 "输血前血压" decimal (5,
 0) DEFAULT NULL,
 "输血前指标检测人员姓名" varchar (50) DEFAULT NULL,
 "输血后体温" decimal (5,
 0) DEFAULT NULL,
 "输血后心率" decimal (3,
 0) DEFAULT NULL,
 "输血后呼吸频率" decimal (3,
 0) DEFAULT NULL,
 "输血后血压" decimal (5,
 0) DEFAULT NULL,
 "输血后指标检测人员姓名" varchar (50) DEFAULT NULL,
 "输血前血红蛋白" decimal (50,
 0) DEFAULT NULL,
 "输血前红细胞比容" decimal (50,
 0) DEFAULT NULL,
 "输血前红细胞数" decimal (50,
 0) DEFAULT NULL,
 "输血前白细胞数" decimal (50,
 0) DEFAULT NULL,
 "输血前血小板值" decimal (50,
 0) DEFAULT NULL,
 "输血前凝血酶原时间" decimal (50,
 0) DEFAULT NULL,
 "输血前活化部分凝血活酶时间" decimal (50,
 0) DEFAULT NULL,
 "输血前纤维蛋白原" decimal (50,
 0) DEFAULT NULL,
 "输血后血红蛋白" decimal (50,
 0) DEFAULT NULL,
 "输血后红细胞比容" decimal (50,
 0) DEFAULT NULL,
 "输血后红细胞数" decimal (50,
 0) DEFAULT NULL,
 "输血后白细胞数" decimal (50,
 0) DEFAULT NULL,
 "输血后血小板值" decimal (50,
 0) DEFAULT NULL,
 "输血后凝血酶原时间" decimal (50,
 0) DEFAULT NULL,
 "输血后活化部分凝血活酶时间" decimal (50,
 0) DEFAULT NULL,
 "输血后纤维蛋白原" decimal (50,
 0) DEFAULT NULL,
 "输血反应标志" varchar (1) DEFAULT NULL,
 "输血反应类型代码" varchar (1) DEFAULT NULL,
 "输血反应类型名称" varchar (50) DEFAULT NULL,
 "不良反应描述" varchar (500) DEFAULT NULL,
 "不良反应处理方法" varchar (500) DEFAULT NULL,
 "输血原因" varchar (100) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 CONSTRAINT "输血记录表"_"记录流水号"_"医疗机构代码"_PK PRIMARY KEY ("记录流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "门(急)诊发药记录" IS '门急诊的发药信息，包括药品基本信息、发药人、发药时间、退药情况等';
COMMENT ON COLUMN "门(急)诊发药记录"."就诊流水号" IS '按照某一特定编码规则赋予特定业务事件的唯一标识，如门急诊就诊流水号、住院就诊流水号。对门诊类型，一般可采用挂号时HIS产生的就诊流水号；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "门(急)诊发药记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "门(急)诊发药记录"."药品代码" IS '按照机构内编码规则赋予登记药品的唯一标识';
COMMENT ON COLUMN "门(急)诊发药记录"."药品采购码" IS '参见附件《药品编码-YPID5》';
COMMENT ON COLUMN "门(急)诊发药记录"."药物名称" IS '药品在特定编码体系中的名称。对于医疗机构制剂，此处填写医疗机构制剂名称';
COMMENT ON COLUMN "门(急)诊发药记录"."医保代码" IS '按照医保编码规则赋予登记药品的唯一标识';
COMMENT ON COLUMN "门(急)诊发药记录"."药物规格" IS '药物规格的描述，如0.25g、5mg×28片/盒';
COMMENT ON COLUMN "门(急)诊发药记录"."药品发药数量" IS '处方中药品的总量';
COMMENT ON COLUMN "门(急)诊发药记录"."药品发药数量单位" IS '发药数量单位在特定编码体系中的名称，如剂、盒等';
COMMENT ON COLUMN "门(急)诊发药记录"."处方编号" IS '按照某一特定编码规则赋予门(急)诊处方的顺序号';
COMMENT ON COLUMN "门(急)诊发药记录"."配药人工号" IS '配药人在原始特定编码体系中的编号';
COMMENT ON COLUMN "门(急)诊发药记录"."配药人姓名" IS '配药人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "门(急)诊发药记录"."配药时间" IS '配药完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门(急)诊发药记录"."发药人工号" IS '发药人在原始特定编码体系中的编号';
COMMENT ON COLUMN "门(急)诊发药记录"."发药人姓名" IS '发药人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "门(急)诊发药记录"."发药时间" IS '发药完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门(急)诊发药记录"."药品批号" IS '按照某一特定编码规则赋予药物生产批号的唯一标志';
COMMENT ON COLUMN "门(急)诊发药记录"."药品批次" IS '药品的生产批号及有效期属性';
COMMENT ON COLUMN "门(急)诊发药记录"."退药标志" IS '标识药品是否退药的标志';
COMMENT ON COLUMN "门(急)诊发药记录"."退药时间" IS '退药完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门(急)诊发药记录"."退药人工号" IS '退药人在特定编码体系中的编号';
COMMENT ON COLUMN "门(急)诊发药记录"."退药人姓名" IS '退药人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "门(急)诊发药记录"."药房代码" IS '医院内部药房代码';
COMMENT ON COLUMN "门(急)诊发药记录"."药房名称" IS '医院内部药房名称';
COMMENT ON COLUMN "门(急)诊发药记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "门(急)诊发药记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门(急)诊发药记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门(急)诊发药记录"."发药序号" IS '按照某一特定编码规则赋予发药顺序的唯一标识';
COMMENT ON COLUMN "门(急)诊发药记录"."医疗机构代码" IS '医疗机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "门(急)诊发药记录"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "门(急)诊发药记录"."发药明细流水号" IS '按照某一特性编码规则赋予发药明细流水号的唯一标识';
COMMENT ON COLUMN "门(急)诊发药记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "门(急)诊发药记录"."就诊科室代码" IS '按照机构内编码规则赋予就诊科室的唯一标识';
COMMENT ON COLUMN "门(急)诊发药记录"."就诊科室名称" IS '就诊科室的机构内名称';
CREATE TABLE IF NOT EXISTS "门(
急)诊发药记录" ("就诊流水号" varchar (32) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "药品代码" varchar (50) DEFAULT NULL,
 "药品采购码" varchar (30) DEFAULT NULL,
 "药物名称" varchar (50) DEFAULT NULL,
 "医保代码" varchar (50) DEFAULT NULL,
 "药物规格" varchar (200) DEFAULT NULL,
 "药品发药数量" decimal (10,
 2) DEFAULT NULL,
 "药品发药数量单位" varchar (100) DEFAULT NULL,
 "处方编号" varchar (32) DEFAULT NULL,
 "配药人工号" varchar (20) DEFAULT NULL,
 "配药人姓名" varchar (50) DEFAULT NULL,
 "配药时间" timestamp DEFAULT NULL,
 "发药人工号" varchar (20) DEFAULT NULL,
 "发药人姓名" varchar (50) DEFAULT NULL,
 "发药时间" timestamp DEFAULT NULL,
 "药品批号" varchar (32) DEFAULT NULL,
 "药品批次" varchar (32) DEFAULT NULL,
 "退药标志" varchar (1) DEFAULT NULL,
 "退药时间" timestamp DEFAULT NULL,
 "退药人工号" varchar (20) DEFAULT NULL,
 "退药人姓名" varchar (50) DEFAULT NULL,
 "药房代码" varchar (32) DEFAULT NULL,
 "药房名称" varchar (100) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "发药序号" varchar (32) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "发药明细流水号" varchar (32) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 CONSTRAINT "门(急)诊发药记录"_"医疗机构代码"_"发药明细流水号"_PK PRIMARY KEY ("医疗机构代码",
 "发药明细流水号")
);


COMMENT ON TABLE "门急诊工作量综合统计表(日报)" IS '医院日门急诊工作量，包括出诊次数、普通门诊人次等';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."社区服务人次" IS '统计日期内社区服务人次';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."急诊抢救人次" IS '统计日期内急诊抢救人次';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."急诊抢救成功人次" IS '统计日期内急诊抢救成功人次';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."死亡人数" IS '统计日期内死亡总人数';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."急诊室死亡人数" IS '统计日期内未收入观察室而在急诊室治疗过程中死亡的人数，不包括来院已死亡人数';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."观察室死亡人数" IS '统计日期内收入观察室后死亡的人数，包括收入观察室不足一天即死亡的病人';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."来院已死人数" IS '统计日期内来院时已无呼吸、脉搏、心跳等生命现象的人数';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."家庭卫生服务人次数" IS '统计日期内家庭卫生服务人次';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."医疗机构代码" IS '填报医疗机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."科室代码" IS '按照机构内编码规则赋予科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."业务日期" IS '业务发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."科室名称" IS '科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."平台科室代码" IS '按照特定编码规则赋予科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."平台科室名称" IS '科室在特定编码体系中的名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."出诊人次" IS '统计日期内医师赴病人家庭或工作地点的诊疗人次数，以及医师定期或临时安排到所属社区进行巡回医疗的诊疗人次数';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."家床人次" IS '统计日期内医护人员按制度赴家庭病床对病人进行诊疗的服务总次数，以挂号为统计依据。';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."其他诊疗人次" IS '统计日期内出上述类别外的诊疗人次数';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."副高以上职称临床医师出诊普通门诊人次" IS '统计日期内副高以上职称临床医师出诊普通门诊人次';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."副高以上职称临床医师出诊门诊总人次" IS '统计日期内副高以上职称临床医师出诊门诊总人次';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."健康检查人次数(健康体检)" IS '统计日期内健康检查(健康体检)人次数';
COMMENT ON COLUMN "门急诊工作量综合统计表(日报)"."职业健康检查人次数" IS '统计日期内健康检查(健康体检)中进行职业健康检查的人次数';
CREATE TABLE IF NOT EXISTS "门急诊工作量综合统计表(
日报)" ("社区服务人次" decimal (15,
 0) DEFAULT NULL,
 "急诊抢救人次" decimal (15,
 0) DEFAULT NULL,
 "急诊抢救成功人次" decimal (15,
 0) DEFAULT NULL,
 "死亡人数" decimal (15,
 0) DEFAULT NULL,
 "急诊室死亡人数" decimal (15,
 0) DEFAULT NULL,
 "观察室死亡人数" decimal (15,
 0) DEFAULT NULL,
 "来院已死人数" decimal (15,
 0) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "家庭卫生服务人次数" decimal (15,
 0) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "科室代码" varchar (20) NOT NULL,
 "业务日期" date NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "平台科室代码" varchar (20) DEFAULT NULL,
 "平台科室名称" varchar (100) DEFAULT NULL,
 "出诊人次" decimal (15,
 0) DEFAULT NULL,
 "家床人次" decimal (15,
 0) DEFAULT NULL,
 "其他诊疗人次" decimal (15,
 0) DEFAULT NULL,
 "副高以上职称临床医师出诊普通门诊人次" decimal (15,
 0) DEFAULT NULL,
 "副高以上职称临床医师出诊门诊总人次" decimal (15,
 0) DEFAULT NULL,
 "健康检查人次数(健康体检)" decimal (15,
 0) DEFAULT NULL,
 "职业健康检查人次数" decimal (15,
 0) DEFAULT NULL,
 CONSTRAINT "门急诊工作量综合统计表(日报)"_"医疗机构代码"_"科室代码"_"业务日期"_PK PRIMARY KEY ("医疗机构代码",
 "科室代码",
 "业务日期")
);


COMMENT ON TABLE "门诊业务汇总" IS '按科室统计每日医院业务量，包括门诊人次、体检人次、出关人次等';
COMMENT ON COLUMN "门诊业务汇总"."急诊人数" IS '业务交易日期内的急诊就诊总人数';
COMMENT ON COLUMN "门诊业务汇总"."门诊就诊人数" IS '业务交易日期内的门诊就诊总人数';
COMMENT ON COLUMN "门诊业务汇总"."门诊总收入" IS '业务交易日期内的门诊总收入';
COMMENT ON COLUMN "门诊业务汇总"."门诊现金收入" IS '业务交易日期内的门诊总收入中现金收入的部分';
COMMENT ON COLUMN "门诊业务汇总"."门诊医保收入" IS '业务交易日期内的门诊总收入中医保收入的部分';
COMMENT ON COLUMN "门诊业务汇总"."门诊处方数" IS '业务交易日期内的门诊开具的处方总数';
COMMENT ON COLUMN "门诊业务汇总"."门诊收费明细记录数" IS '业务交易日期内的门诊收费明细的记录总数';
COMMENT ON COLUMN "门诊业务汇总"."诊疗费" IS '业务交易日期内的门诊诊疗费用总和';
COMMENT ON COLUMN "门诊业务汇总"."治疗费" IS '业务交易日期内的门诊治疗费用总和';
COMMENT ON COLUMN "门诊业务汇总"."手术材料费" IS '业务交易日期内的门诊手术材料费用总和';
COMMENT ON COLUMN "门诊业务汇总"."检查费" IS '业务交易日期内的门诊检查费用总和';
COMMENT ON COLUMN "门诊业务汇总"."化验费" IS '业务交易日期内的门诊化验费用总和';
COMMENT ON COLUMN "门诊业务汇总"."摄片费" IS '业务交易日期内的门诊摄片费用总和';
COMMENT ON COLUMN "门诊业务汇总"."透视费" IS '业务交易日期内的门诊透视费用总和';
COMMENT ON COLUMN "门诊业务汇总"."西药费" IS '业务交易日期内的门诊西药费用总和';
COMMENT ON COLUMN "门诊业务汇总"."中成药费" IS '业务交易日期内的门诊中成药费用总和';
COMMENT ON COLUMN "门诊业务汇总"."中草药费" IS '业务交易日期内的门诊中草药费用总和';
COMMENT ON COLUMN "门诊业务汇总"."其它费" IS '业务交易日期内的门诊除上述费用外的其它费用总和';
COMMENT ON COLUMN "门诊业务汇总"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "门诊业务汇总"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门诊业务汇总"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "门诊业务汇总"."医疗机构代码" IS '填报医疗机构按照机构内编码体系填写的唯一标识';
COMMENT ON COLUMN "门诊业务汇总"."医疗机构名称" IS '就诊医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "门诊业务汇总"."业务日期" IS '业务交易发生的公元纪年日期，即统计日期';
COMMENT ON COLUMN "门诊业务汇总"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "门诊业务汇总"."门诊就诊人次" IS '业务交易日期内的门诊就诊总人次';
COMMENT ON COLUMN "门诊业务汇总"."急诊人次" IS '业务交易日期内的急诊就诊总人次';
CREATE TABLE IF NOT EXISTS "门诊业务汇总" (
"急诊人数" decimal (5,
 0) DEFAULT NULL,
 "门诊就诊人数" decimal (5,
 0) DEFAULT NULL,
 "门诊总收入" decimal (18,
 3) DEFAULT NULL,
 "门诊现金收入" decimal (18,
 3) DEFAULT NULL,
 "门诊医保收入" decimal (18,
 3) DEFAULT NULL,
 "门诊处方数" decimal (10,
 0) DEFAULT NULL,
 "门诊收费明细记录数" decimal (10,
 0) DEFAULT NULL,
 "诊疗费" decimal (10,
 3) DEFAULT NULL,
 "治疗费" decimal (18,
 3) DEFAULT NULL,
 "手术材料费" decimal (18,
 3) DEFAULT NULL,
 "检查费" decimal (18,
 3) DEFAULT NULL,
 "化验费" decimal (18,
 3) DEFAULT NULL,
 "摄片费" decimal (18,
 3) DEFAULT NULL,
 "透视费" decimal (18,
 3) DEFAULT NULL,
 "西药费" decimal (18,
 3) DEFAULT NULL,
 "中成药费" decimal (18,
 3) DEFAULT NULL,
 "中草药费" decimal (18,
 3) DEFAULT NULL,
 "其它费" decimal (18,
 3) DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "业务日期" date NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "门诊就诊人次" decimal (5,
 0) DEFAULT NULL,
 "急诊人次" decimal (5,
 0) DEFAULT NULL,
 CONSTRAINT "门诊业务汇总"_"医疗机构代码"_"业务日期"_PK PRIMARY KEY ("医疗机构代码",
 "业务日期")
);


COMMENT ON TABLE "阶段小结" IS '针对住院时间较长的患者，医师每月关于患者主诉、诊断、治疗经过、诊疗计划等信息的记录';
COMMENT ON COLUMN "阶段小结"."中药煎煮方法" IS '中药饮片煎煮方法描述，如水煎等';
COMMENT ON COLUMN "阶段小结"."治则治法代码" IS '辩证结果采用的治则治法在特定编码体系中的代码。如有多条，用“，”加以分隔';
COMMENT ON COLUMN "阶段小结"."中医“四诊”观察结果" IS '中医“四诊”观察结果的详细描述，包括望、闻、问、切四诊内容';
COMMENT ON COLUMN "阶段小结"."目前诊断-中医症候名称" IS '目前诊断中医证候标准名称';
COMMENT ON COLUMN "阶段小结"."目前诊断-中医症候代码" IS '患者入院时按照平台编码规则赋予目前诊断中医证候的唯一标识';
COMMENT ON COLUMN "阶段小结"."目前诊断-中医病名名称" IS '患者入院时按照平台编码规则赋予目前诊断中医疾病的唯一标识';
COMMENT ON COLUMN "阶段小结"."目前诊断-中医病名代码" IS '患者入院时按照中医病名特定分类体系中特定编码规则赋予目前诊断中医疾病的唯一标识';
COMMENT ON COLUMN "阶段小结"."目前诊断-西医诊断名称" IS '目前诊断西医诊断标准名称';
COMMENT ON COLUMN "阶段小结"."目前诊断-西医诊断代码" IS '按照特定编码规则赋予当前诊断疾病的唯一标识';
COMMENT ON COLUMN "阶段小结"."入院诊断-中医症候名称" IS '由医师根据患者人院时的情况，综合分析所作出的标准中医证候名称';
COMMENT ON COLUMN "阶段小结"."入院诊断-中医症候代码" IS '患者入院时按照平台编码规则赋予初步诊断中医证候的唯一标识';
COMMENT ON COLUMN "阶段小结"."入院诊断-中医病名名称" IS '由医师根据患者人院时的情况，综合分析所作出的中医疾病标准名称';
COMMENT ON COLUMN "阶段小结"."入院诊断-中医病名代码" IS '患者入院时按照平台编码规则赋予初步诊断中医疾病的唯一标识';
COMMENT ON COLUMN "阶段小结"."入院诊断-西医诊断名称" IS '由医师根据患者入院时的情况，综合分析所作出的西医诊断标准名称';
COMMENT ON COLUMN "阶段小结"."入院诊断-西医诊断代码" IS '患者入院时按照平台编码规则赋予西医初步诊断疾病的唯一标识';
COMMENT ON COLUMN "阶段小结"."小结时间" IS '记录小结完成的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "阶段小结"."入院时间" IS '患者实际办理入院手续当日的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "阶段小结"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "阶段小结"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "阶段小结"."年龄(月)" IS '年龄不足1周岁的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "阶段小结"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写';
COMMENT ON COLUMN "阶段小结"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "阶段小结"."性别名称" IS '一般指患者医学生理性别，指男性或女性';
COMMENT ON COLUMN "阶段小结"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "阶段小结"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "阶段小结"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "阶段小结"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "阶段小结"."病区名称" IS '患者当前所住病区的名称';
COMMENT ON COLUMN "阶段小结"."科室名称" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的名称';
COMMENT ON COLUMN "阶段小结"."科室代码" IS '患者在医疗机构就诊的科室在原始采集机构编码体系中的代码';
COMMENT ON COLUMN "阶段小结"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "阶段小结"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "阶段小结"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "阶段小结"."住院次数" IS '办理完整住院治疗手续的次数';
COMMENT ON COLUMN "阶段小结"."住院就诊流水号" IS '按照某一特定编码规则赋予住院就诊事件的唯一标识；对住院类型,可用入院登记时HIS中产生的“住院号”。';
COMMENT ON COLUMN "阶段小结"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "阶段小结"."阶段小结流水号" IS '按照某一特性编码规则赋予阶段小结记录的唯一标识';
COMMENT ON COLUMN "阶段小结"."医疗机构名称" IS '取得医疗机构执业许可证并开展诊疗活动的机构名，这里指本次就诊的医疗机构名称';
COMMENT ON COLUMN "阶段小结"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "阶段小结"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "阶段小结"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "阶段小结"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "阶段小结"."签名时间" IS '医师姓名完成时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "阶段小结"."医师姓名" IS '医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "阶段小结"."医师工号" IS '负责住院小结的医师的工号';
COMMENT ON COLUMN "阶段小结"."今后治疗方案" IS '今后治疗方案的详细描述，包含中医及民族医治疗项目';
COMMENT ON COLUMN "阶段小结"."目前情况" IS '对患者当前情况的详细描述';
COMMENT ON COLUMN "阶段小结"."诊疗过程描述" IS '对患者诊疗过程或抢救情况的详细描述';
COMMENT ON COLUMN "阶段小结"."医嘱内容" IS '医嘱内容的详细描述';
COMMENT ON COLUMN "阶段小结"."入院情况" IS '对患者入院情况的详细描述';
COMMENT ON COLUMN "阶段小结"."主诉" IS '对患者本次疾病相关的主要症状及其持续时间的描述，一般由患者本人或监护人描述';
COMMENT ON COLUMN "阶段小结"."中药用药方法" IS '中药的用药方法的描述，如bid 煎服，先煎、后下等';
CREATE TABLE IF NOT EXISTS "阶段小结" (
"中药煎煮方法" varchar (100) DEFAULT NULL,
 "治则治法代码" varchar (100) DEFAULT NULL,
 "中医“四诊”观察结果" text,
 "目前诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "目前诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "目前诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "目前诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "目前诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "目前诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "入院诊断-中医症候名称" varchar (512) DEFAULT NULL,
 "入院诊断-中医症候代码" varchar (64) DEFAULT NULL,
 "入院诊断-中医病名名称" varchar (512) DEFAULT NULL,
 "入院诊断-中医病名代码" varchar (64) DEFAULT NULL,
 "入院诊断-西医诊断名称" varchar (512) DEFAULT NULL,
 "入院诊断-西医诊断代码" varchar (64) DEFAULT NULL,
 "小结时间" timestamp DEFAULT NULL,
 "入院时间" timestamp DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "阶段小结流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "医师姓名" varchar (50) DEFAULT NULL,
 "医师工号" varchar (20) DEFAULT NULL,
 "今后治疗方案" text,
 "目前情况" text,
 "诊疗过程描述" text,
 "医嘱内容" text,
 "入院情况" text,
 "主诉" text,
 "中药用药方法" varchar (100) DEFAULT NULL,
 CONSTRAINT "阶段小结"_"阶段小结流水号"_"医疗机构代码"_PK PRIMARY KEY ("阶段小结流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "颅颌面畸形颅面外科矫治术记录" IS '颅颌面畸形颅面外科矫治术记录，包括术前各项检查结果、手术名称、时间、术后各种指标情况';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术结束时间" IS '对患者结束手术操作时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."其他手术方式" IS '其他手术方式的详细描述';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."面神经标志" IS '标识是否面神经的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."面神经具体分支" IS '面神经具体分支的分类代码';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."视神经标志" IS '标识是否视神经的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."下槽牙神经标志" IS '标识是否下槽牙神经的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."术后创口感染标志" IS '标识是否术后创口感染的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."术后天数" IS '术后具体天数，计量单位为天';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."细菌培养" IS '细菌培养结果描述';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."治愈天数" IS '治愈后的具体天数，计量单位为天';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."术后钛板钛钉松动脱落标志" IS '标识是否术后钛板钛钉松动脱落的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."钛板钛钉详细情况" IS '钛板钛钉详细情况的详细描述';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."术后钛板折断标志" IS '标识是否术后钛板折断的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."钛板折断详细情况" IS '钛板折断详细情况的详细描述';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."脑脊液漏标志" IS '有无脑脊液漏的情况标识';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."颅内感染标志" IS '有无颅内感染的情况标识';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."复查口腔全景片标志" IS '标识是否复查口腔全景片的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."复查上下颌骨CT标志" IS '标识是否复查上下颌骨CT的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."住院期间非计划二次手术标志" IS '有无住院期间非计划二次手术的情况标识';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."二次手术详细" IS '二次手术详细在特定编码体系中的代码';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."术后抢救标志" IS '标识是否术后抢救的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."抢救结果" IS '抢救结果的分类代码';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."术中术后死亡标志" IS '标识是否术中术后死亡的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师工号1" IS '手术医师1在原始特定编码体系中的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师姓名1" IS '手术医师1在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师工号2" IS '手术医师2在原始特定编码体系中的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师姓名2" IS '手术医师2在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师工号3" IS '手术医师3在原始特定编码体系中的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师姓名3" IS '手术医师3在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师工号4" IS '手术医师4在原始特定编码体系中的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师姓名4" IS '手术医师4在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师工号5" IS '手术医师5在原始特定编码体系中的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师姓名5" IS '手术医师5在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师工号6" IS '手术医师6在原始特定编码体系中的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师姓名6" IS '手术医师6在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师工号7" IS '手术医师7在原始特定编码体系中的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师姓名7" IS '手术医师7在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师工号8" IS '手术医师8在原始特定编码体系中的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师姓名8" IS '手术医师8在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师工号9" IS '手术医师9在原始特定编码体系中的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师姓名9" IS '手术医师9在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师工号10" IS '手术医师10在原始特定编码体系中的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术医师姓名10" IS '手术医师10在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."填表人工号" IS '填表人在原始特定编码体系中的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."填表人姓名" IS '填表人在公安管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."填表时间" IS '完成填表时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术记录流水号" IS '按照某一特定编码规则赋予手术记录的唯一标识';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."矫治术记录编号" IS '按照某一特定编码规则赋予矫治术记录的唯一标识';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."医疗机构代码" IS '医疗机构在国家直报系统中的 12 位编码（如： 520000000001）';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术方式" IS '手术方式的分类代码';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术申请单号" IS '按照某一特定编码规则赋予手术申请单的唯一标识';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."就诊科室代码" IS '按照机构内编码规则赋予所在科室(如内科、外科、儿科、妇科、眼科、耳鼻喉科等)的唯一标识';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."就诊科室名称" IS '所在科室机构内名称，如内科、外科、儿科、妇科、眼科、耳鼻喉科等';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."病区名称" IS '患者入院时，所住病区名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."病房号" IS '患者入院时，所住病房对应的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."病床号" IS '患者入院时，所住床位对应的编号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."病案号" IS '按照某一特定编码规则赋予个体在医疗机构住院或建立家庭病床的病案号。原则上，同一患者在同一医疗机构多次住院或建立家庭病床应当使用同一病案号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."住院号" IS '按照某一特定编码规则赋予住院就诊对象的顺序号。原则上，同一患者在同一医疗机构多次住院应当使用同一住院号';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."住院次数" IS '此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."性别代码" IS '生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."出生日期" IS '患者出生当日的公元纪年日期';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."ABO血型代码" IS '受检者按照ABO血型系统决定的血型在特定编码体系中的代码';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."Rh血型代码" IS '进行血型检查明确，或既往病历资料能够明确的患者Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."口腔颌面外科标志" IS '标识是否为口腔颌面外科的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."安氏分类" IS '安氏分类的特定代码';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."安氏分类其他" IS '其他安氏分类详细描述';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."术前正畸标志" IS '标识是否术前正畸的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."术前头颅正侧位头影测量标志" IS '标识是否术前头颅正侧位头影测量的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."术前上下颌CT标志" IS '标识是否术前上下颌CT的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."术前计算机辅助标志" IS '标识是否术前计算机辅助的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."术前模型外科标志" IS '标识是否术前模型外科的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."术前使用导板制备标志" IS '标识是否术前使用导板制备的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."住院天数" IS '患者实际的住院天数，入院日与出院日只计算1天';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."住院费" IS '住院总费用，计量单位为人民币元';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."入住ICU标志" IS '标识是否入住ICU的标志';
COMMENT ON COLUMN "颅颌面畸形颅面外科矫治术记录"."手术开始时间" IS '以手术操作刀碰皮(切皮)时点计算，经自然腔道到达手术部位，没有皮肤切口的手术，开始时间以手术器械进入人体腔道的时点为准,对患者开始手术操作时的公元纪年日期和时间的完整描述。';
CREATE TABLE IF NOT EXISTS "颅颌面畸形颅面外科矫治术记录" (
"手术结束时间" timestamp DEFAULT NULL,
 "其他手术方式" varchar (200) DEFAULT NULL,
 "面神经标志" varchar (1) DEFAULT NULL,
 "面神经具体分支" decimal (1,
 0) DEFAULT NULL,
 "视神经标志" varchar (1) DEFAULT NULL,
 "下槽牙神经标志" varchar (1) DEFAULT NULL,
 "术后创口感染标志" varchar (1) DEFAULT NULL,
 "术后天数" decimal (10,
 0) DEFAULT NULL,
 "细菌培养" varchar (100) DEFAULT NULL,
 "治愈天数" decimal (10,
 0) DEFAULT NULL,
 "术后钛板钛钉松动脱落标志" varchar (1) DEFAULT NULL,
 "钛板钛钉详细情况" varchar (100) DEFAULT NULL,
 "术后钛板折断标志" varchar (1) DEFAULT NULL,
 "钛板折断详细情况" varchar (100) DEFAULT NULL,
 "脑脊液漏标志" varchar (1) DEFAULT NULL,
 "颅内感染标志" varchar (1) DEFAULT NULL,
 "复查口腔全景片标志" varchar (1) DEFAULT NULL,
 "复查上下颌骨CT标志" varchar (1) DEFAULT NULL,
 "住院期间非计划二次手术标志" varchar (1) DEFAULT NULL,
 "二次手术详细" decimal (1,
 0) DEFAULT NULL,
 "术后抢救标志" varchar (1) DEFAULT NULL,
 "抢救结果" decimal (1,
 0) DEFAULT NULL,
 "术中术后死亡标志" varchar (1) DEFAULT NULL,
 "手术医师工号1" varchar (20) DEFAULT NULL,
 "手术医师姓名1" varchar (50) DEFAULT NULL,
 "手术医师工号2" varchar (20) DEFAULT NULL,
 "手术医师姓名2" varchar (50) DEFAULT NULL,
 "手术医师工号3" varchar (20) DEFAULT NULL,
 "手术医师姓名3" varchar (50) DEFAULT NULL,
 "手术医师工号4" varchar (20) DEFAULT NULL,
 "手术医师姓名4" varchar (50) DEFAULT NULL,
 "手术医师工号5" varchar (20) DEFAULT NULL,
 "手术医师姓名5" varchar (50) DEFAULT NULL,
 "手术医师工号6" varchar (20) DEFAULT NULL,
 "手术医师姓名6" varchar (50) DEFAULT NULL,
 "手术医师工号7" varchar (20) DEFAULT NULL,
 "手术医师姓名7" varchar (50) DEFAULT NULL,
 "手术医师工号8" varchar (20) DEFAULT NULL,
 "手术医师姓名8" varchar (50) DEFAULT NULL,
 "手术医师工号9" varchar (20) DEFAULT NULL,
 "手术医师姓名9" varchar (50) DEFAULT NULL,
 "手术医师工号10" varchar (20) DEFAULT NULL,
 "手术医师姓名10" varchar (50) DEFAULT NULL,
 "填表人工号" varchar (20) DEFAULT NULL,
 "填表人姓名" varchar (50) DEFAULT NULL,
 "填表时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "手术记录流水号" varchar (64) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "矫治术记录编号" varchar (32) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "手术方式" varchar (30) DEFAULT NULL,
 "手术申请单号" varchar (32) DEFAULT NULL,
 "就诊科室代码" varchar (20) DEFAULT NULL,
 "就诊科室名称" varchar (100) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "病案号" varchar (50) DEFAULT NULL,
 "住院号" varchar (32) DEFAULT NULL,
 "住院次数" decimal (5,
 0) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "口腔颌面外科标志" varchar (1) DEFAULT NULL,
 "安氏分类" decimal (1,
 0) DEFAULT NULL,
 "安氏分类其他" varchar (200) DEFAULT NULL,
 "术前正畸标志" varchar (1) DEFAULT NULL,
 "术前头颅正侧位头影测量标志" varchar (1) DEFAULT NULL,
 "术前上下颌CT标志" varchar (1) DEFAULT NULL,
 "术前计算机辅助标志" varchar (1) DEFAULT NULL,
 "术前模型外科标志" varchar (1) DEFAULT NULL,
 "术前使用导板制备标志" varchar (1) DEFAULT NULL,
 "住院天数" decimal (5,
 0) DEFAULT NULL,
 "住院费" decimal (18,
 3) DEFAULT NULL,
 "入住ICU标志" varchar (1) DEFAULT NULL,
 "手术开始时间" timestamp DEFAULT NULL,
 CONSTRAINT "颅颌面畸形颅面外科矫治术记录"_"矫治术记录编号"_"医疗机构代码"_PK PRIMARY KEY ("矫治术记录编号",
 "医疗机构代码")
);


COMMENT ON TABLE "高值耗材使用记录" IS '患者本次就诊高值耗材使用记录，包括疾病、耗材以及使用时间';
COMMENT ON COLUMN "高值耗材使用记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "高值耗材使用记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "高值耗材使用记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "高值耗材使用记录"."使用时间" IS '高值耗材使用当日的公元纪年和日期的完整描述';
COMMENT ON COLUMN "高值耗材使用记录"."医生姓名" IS '使用医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "高值耗材使用记录"."医生工号" IS '使用医师在特定编码体系中的工号';
COMMENT ON COLUMN "高值耗材使用记录"."使用途径" IS '医用耗材使用途径的描述';
COMMENT ON COLUMN "高值耗材使用记录"."耗材金额" IS '医用耗材的总金额，计量单位为人民币元';
COMMENT ON COLUMN "高值耗材使用记录"."耗材单价" IS '医用耗材的单价，计量单位为元';
COMMENT ON COLUMN "高值耗材使用记录"."耗材单位" IS '医用耗材总量对应的计量单位';
COMMENT ON COLUMN "高值耗材使用记录"."数量" IS '医用耗材的总量';
COMMENT ON COLUMN "高值耗材使用记录"."材料名称" IS '医用耗材在标准编码体系中的名称';
COMMENT ON COLUMN "高值耗材使用记录"."材料编号" IS '医用耗材在标准编码体系中的代码';
COMMENT ON COLUMN "高值耗材使用记录"."是否植入性耗材" IS '标志是否为植入性耗材的标识';
COMMENT ON COLUMN "高值耗材使用记录"."诊断疾病名称" IS '疾病诊断在标准编码体系中的名称。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "高值耗材使用记录"."诊断疾病代码" IS '疾病诊断在标准编码体系中的代码。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "高值耗材使用记录"."中医/西医诊断标志" IS '诊断分类(如西医诊断、中医症候、中医疾病等)在标准编码体系中的代码';
COMMENT ON COLUMN "高值耗材使用记录"."门诊/急诊/住院流水号" IS '按照某一特定编码规则赋予就诊事件的唯一标识，同门急诊/住院关联';
COMMENT ON COLUMN "高值耗材使用记录"."门诊/急诊/住院标识" IS '患者就诊类别在标准体系中的代码';
COMMENT ON COLUMN "高值耗材使用记录"."出生日期" IS '个体出生当日的公元纪年日期';
COMMENT ON COLUMN "高值耗材使用记录"."性别" IS '个体生理性别在标准编码体系中的代码';
COMMENT ON COLUMN "高值耗材使用记录"."姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "高值耗材使用记录"."证件号码" IS '个体各类型身份证件上的唯一法定标识符';
COMMENT ON COLUMN "高值耗材使用记录"."证件类型代码" IS '个体身份证件所属类别在标准编码体系中的代码';
COMMENT ON COLUMN "高值耗材使用记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "高值耗材使用记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在标准编码体系中的代码';
COMMENT ON COLUMN "高值耗材使用记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "高值耗材使用记录"."耗材使用流水号" IS '按照特定编码规则赋予耗材使用记录唯一标识的顺序号';
COMMENT ON COLUMN "高值耗材使用记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识。这里指医疗机构组织机构代码';
CREATE TABLE IF NOT EXISTS "高值耗材使用记录" (
"密级" varchar (16) DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "使用时间" timestamp DEFAULT NULL,
 "医生姓名" varchar (50) DEFAULT NULL,
 "医生工号" varchar (20) DEFAULT NULL,
 "使用途径" varchar (100) DEFAULT NULL,
 "耗材金额" decimal (18,
 2) DEFAULT NULL,
 "耗材单价" decimal (18,
 2) DEFAULT NULL,
 "耗材单位" varchar (20) DEFAULT NULL,
 "数量" decimal (5,
 0) DEFAULT NULL,
 "材料名称" varchar (100) DEFAULT NULL,
 "材料编号" varchar (12) DEFAULT NULL,
 "是否植入性耗材" varchar (1) DEFAULT NULL,
 "诊断疾病名称" varchar (200) DEFAULT NULL,
 "诊断疾病代码" varchar (20) DEFAULT NULL,
 "中医/西医诊断标志" varchar (1) DEFAULT NULL,
 "门诊/急诊/住院流水号" varchar (32) DEFAULT NULL,
 "门诊/急诊/住院标识" varchar (1) DEFAULT NULL,
 "出生日期" date DEFAULT NULL,
 "性别" varchar (2) DEFAULT NULL,
 "姓名" varchar (50) DEFAULT NULL,
 "证件号码" varchar (32) DEFAULT NULL,
 "证件类型代码" varchar (2) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "耗材使用流水号" varchar (32) NOT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 CONSTRAINT "高值耗材使用记录"_"耗材使用流水号"_"医疗机构代码"_PK PRIMARY KEY ("耗材使用流水号",
 "医疗机构代码")
);


COMMENT ON TABLE "麻醉术前访视记录" IS '术前访视记录，包括术前各项检查结果、拟实施手术和麻醉信息';
COMMENT ON COLUMN "麻醉术前访视记录"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "麻醉术前访视记录"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "麻醉术前访视记录"."手术间编号" IS '对患者实施手术操作时所在的手术室房间工号';
COMMENT ON COLUMN "麻醉术前访视记录"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "麻醉术前访视记录"."性别代码" IS '个体生理性别在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术前访视记录"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "麻醉术前访视记录"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "麻醉术前访视记录"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "麻醉术前访视记录"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "麻醉术前访视记录"."体重(kg)" IS '体重的测量值，计量单位为kg';
COMMENT ON COLUMN "麻醉术前访视记录"."ABO血型代码" IS '为患者实际输入的ABO血型类别在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术前访视记录"."Rh血型代码" IS '为患者实际输入的Rh血型的类别在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术前访视记录"."术前诊断代码" IS '术前诊断在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术前访视记录"."ASA分级代码" IS '根据美同麻醉师协会(ASA)制定的分级标准，对病人体质状况和对手术危险性进行评估分级的结果在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术前访视记录"."拟实施手术及操作代码" IS '拟实施的手术及操作在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术前访视记录"."拟实施麻醉方法代码" IS '拟为患者进行手术、操作时使用的麻醉方法在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术前访视记录"."术前合并疾病" IS '术前合并疾病的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."注意事项" IS '对可能出现问题及采取相应措施的描述';
COMMENT ON COLUMN "麻醉术前访视记录"."简要病史" IS '对患者病史的简要描述';
COMMENT ON COLUMN "麻醉术前访视记录"."过敏史" IS '患者既往发生过敏情况的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."心电图检査结果" IS '对患者心电图检査结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."胸部X线检查结果" IS '对患者胸部X线检査结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."CT检査结果" IS '检査结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."B超检查结果" IS 'B超检査结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."MRI检查结果" IS 'MRI检查结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."肺功能检查结果" IS '患者肺功能检查结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."血常规检查结果" IS '麻醉术前访视时，对患者血常规检查结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."尿常规检査结果" IS '麻醉术前访视时，对患者尿常规检查结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."凝血功能检查结果" IS '麻醉术前访视时，对患者凝血功能检查结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."肝功能检查结果" IS '对患者肝功能检査结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."血气分析检査结果" IS '麻醉术前访视时，对患者血气分析检査结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."一般状况检查结果" IS '对患者一般状况检査结果的详细描述，包括其发育状况、营养状况、体味、步态、面容与表情、意识，检查能否合作等';
COMMENT ON COLUMN "麻醉术前访视记录"."精神状态正常标志" IS '标识患者精神状态是否正常的标志';
COMMENT ON COLUMN "麻醉术前访视记录"."心脏听诊结果" IS '麻醉术前访视时，对心脏听诊结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."肺部听诊结果" IS '麻醉术前访视时，对患者肺部听诊检查结果的描述';
COMMENT ON COLUMN "麻醉术前访视记录"."四肢检查结果" IS '麻醉术前访视时，对四肢检查结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."脊柱检查结果" IS '麻醉术前访视时，对脊柱检查结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."腹部检查结果" IS '麻醉术前访视时，对腹部检查结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."气管检査结果" IS '麻醉术前访视时，对气管检査结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."牙齿检査结果" IS '麻醉术前访视时，对牙齿检査结果的详细描述';
COMMENT ON COLUMN "麻醉术前访视记录"."术前麻醉医嘱" IS '术前麻醉医师下达的医嘱';
COMMENT ON COLUMN "麻醉术前访视记录"."麻醉适应证" IS '麻醉适应证的描述';
COMMENT ON COLUMN "麻醉术前访视记录"."麻醉医师工号" IS '麻醉医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "麻醉术前访视记录"."麻醉医师姓名" IS '对患者实施麻醉的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "麻醉术前访视记录"."签名时间" IS '麻醉医师完成签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉术前访视记录"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "麻醉术前访视记录"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉术前访视记录"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉术前访视记录"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "麻醉术前访视记录"."麻醉术前访视记录单号" IS '按照特定编码规则赋予麻醉术前访视记录的唯一标志';
COMMENT ON COLUMN "麻醉术前访视记录"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "麻醉术前访视记录"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "麻醉术前访视记录"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术前访视记录"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "麻醉术前访视记录"."门诊就诊流水号" IS '为门诊就诊时，填写门诊就诊流水号，住院就诊流水号填“-”';
COMMENT ON COLUMN "麻醉术前访视记录"."电子申请单编号" IS '按照某一特定编码规则赋予电子申请单的顺序号';
COMMENT ON COLUMN "麻醉术前访视记录"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "麻醉术前访视记录"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "麻醉术前访视记录"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "麻醉术前访视记录"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉术前访视记录"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "麻醉术前访视记录"."病区名称" IS '患者所在病区的名称';
CREATE TABLE IF NOT EXISTS "麻醉术前访视记录" (
"病房号" varchar (10) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "手术间编号" varchar (20) DEFAULT NULL,
 "患者姓名" varchar (50) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "体重(kg)" decimal (6,
 2) DEFAULT NULL,
 "ABO血型代码" varchar (1) DEFAULT NULL,
 "Rh血型代码" varchar (1) DEFAULT NULL,
 "术前诊断代码" varchar (100) DEFAULT NULL,
 "ASA分级代码" decimal (1,
 0) DEFAULT NULL,
 "拟实施手术及操作代码" varchar (50) DEFAULT NULL,
 "拟实施麻醉方法代码" varchar (20) DEFAULT NULL,
 "术前合并疾病" varchar (100) DEFAULT NULL,
 "注意事项" text,
 "简要病史" varchar (100) DEFAULT NULL,
 "过敏史" text,
 "心电图检査结果" varchar (100) DEFAULT NULL,
 "胸部X线检查结果" varchar (100) DEFAULT NULL,
 "CT检査结果" varchar (100) DEFAULT NULL,
 "B超检查结果" varchar (100) DEFAULT NULL,
 "MRI检查结果" text,
 "肺功能检查结果" text,
 "血常规检查结果" text,
 "尿常规检査结果" varchar (100) DEFAULT NULL,
 "凝血功能检查结果" varchar (100) DEFAULT NULL,
 "肝功能检查结果" varchar (100) DEFAULT NULL,
 "血气分析检査结果" text,
 "一般状况检查结果" text,
 "精神状态正常标志" varchar (1) DEFAULT NULL,
 "心脏听诊结果" varchar (100) DEFAULT NULL,
 "肺部听诊结果" varchar (100) DEFAULT NULL,
 "四肢检查结果" text,
 "脊柱检查结果" text,
 "腹部检查结果" text,
 "气管检査结果" varchar (100) DEFAULT NULL,
 "牙齿检査结果" varchar (100) DEFAULT NULL,
 "术前麻醉医嘱" text,
 "麻醉适应证" varchar (100) DEFAULT NULL,
 "麻醉医师工号" varchar (20) DEFAULT NULL,
 "麻醉医师姓名" varchar (50) DEFAULT NULL,
 "签名时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "麻醉术前访视记录单号" varchar (64) NOT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "门诊就诊流水号" varchar (32) DEFAULT NULL,
 "电子申请单编号" varchar (100) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 CONSTRAINT "麻醉术前访视记录"_"麻醉术前访视记录单号"_"医疗机构代码"_PK PRIMARY KEY ("麻醉术前访视记录单号",
 "医疗机构代码")
);


COMMENT ON TABLE "麻醉知情同意书" IS '麻醉方法、麻醉中可能出现意外及风险的情况说明';
COMMENT ON COLUMN "麻醉知情同意书"."患者姓名" IS '患者本人在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "麻醉知情同意书"."就诊次数" IS '对于门(急)诊患者，此处表示患者门(急)诊的累计次数；对于住院患者，此处表示住院次数，即“第次住院”指患者在本医疗机构住院诊治的次数。计量单位为次';
COMMENT ON COLUMN "麻醉知情同意书"."病床号" IS '患者住院期间，所住床位对应的编号';
COMMENT ON COLUMN "麻醉知情同意书"."病房号" IS '按照某一特定编码规则赋予患者住院期间，所住病房对应的编号';
COMMENT ON COLUMN "麻醉知情同意书"."病区名称" IS '患者所在病区的名称';
COMMENT ON COLUMN "麻醉知情同意书"."科室代码" IS '按照机构内编码规则赋予出院科室的唯一标识';
COMMENT ON COLUMN "麻醉知情同意书"."科室名称" IS '出院科室的机构内名称';
COMMENT ON COLUMN "麻醉知情同意书"."知情同意书编号" IS '按照某一特定编码规则赋予知情同意书的唯一标识';
COMMENT ON COLUMN "麻醉知情同意书"."门诊就诊流水号" IS '为门诊就诊时，填写门诊就诊流水号，住院就诊流水号填“-”';
COMMENT ON COLUMN "麻醉知情同意书"."住院就诊流水号" IS '为住院时，填写住院就诊流水号，门诊就诊流水号字段填“-”';
COMMENT ON COLUMN "麻醉知情同意书"."就诊事件类型代码" IS '患者就诊事件类型如门诊、急诊在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉知情同意书"."卡号" IS '按照某一特定编码规则赋予患者就医卡证的唯一标识';
COMMENT ON COLUMN "麻醉知情同意书"."卡类型代码" IS '患者就医卡证的类型(如居民身份证、医保卡、保险证、军官证、医院自制卡证等)在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉知情同意书"."患者唯一标识号" IS '按照某一特定编码规则在某一医疗机构或医院内部分配的、用于唯一标识每个患者的标识符';
COMMENT ON COLUMN "麻醉知情同意书"."修改标志" IS '标识数据是否进行修改的标志，其中：1：正常、2：修改、3：撤销';
COMMENT ON COLUMN "麻醉知情同意书"."麻醉知情同意书流水号" IS '按照某一特定编码规则赋予麻醉知情同意书的唯一标识';
COMMENT ON COLUMN "麻醉知情同意书"."医疗机构名称" IS '医疗机构的组织机构名称，即《医疗机构执业许可证》登记的机构名称。';
COMMENT ON COLUMN "麻醉知情同意书"."医疗机构代码" IS '按照某一特定编码规则赋予就诊医疗机构的唯一标识';
COMMENT ON COLUMN "麻醉知情同意书"."数据更新时间" IS '数据更新时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉知情同意书"."数据采集时间" IS '数据采集时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉知情同意书"."密级" IS '文档保密级别，如无特控、患者申明特控(无特殊原因不予调阅)';
COMMENT ON COLUMN "麻醉知情同意书"."麻醉医师签名时间" IS '麻醉医师进行电子签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉知情同意书"."麻醉医师姓名" IS '对患者实施麻醉的医师在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "麻醉知情同意书"."麻醉医师工号" IS '麻醉医师在机构内特定编码体系中的编号';
COMMENT ON COLUMN "麻醉知情同意书"."患者/法定代理人签名时间" IS '患者或法定代理人签名时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉知情同意书"."法定代理人与患者的关系名称" IS '本人与特定对象的关系类别(如户主、配偶、子女、父母等)名称';
COMMENT ON COLUMN "麻醉知情同意书"."法定代理人与患者的关系代码" IS '本人与特定对象的关系类别(如户主、配偶、子女、父母等)在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉知情同意书"."法定代理人姓名" IS '法定代理人签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "麻醉知情同意书"."患者签名" IS '患者签署的在公安户籍管理部门正式登记注册的姓氏和名称';
COMMENT ON COLUMN "麻醉知情同意书"."患者/法定代理人意见" IS '患者／法定代理人对知情同意书中告知内容的意见描述';
COMMENT ON COLUMN "麻醉知情同意书"."医疗机构意见" IS '在此诊疗过程中，医疗机构对患者应尽责任的陈述以及可能面临的风险或意外情况所采取的应对措施的详细描述';
COMMENT ON COLUMN "麻醉知情同意书"."参加麻醉安全保险标志" IS '标识患者是否同意参加麻醉安全保险的标志';
COMMENT ON COLUMN "麻醉知情同意书"."使用麻醉镇痛泵标志" IS '标识患者是否同意使用麻醉镇痛泵的的标志';
COMMENT ON COLUMN "麻醉知情同意书"."麻醉中、麻醉后可能发生的意外及并发症" IS '麻醉中及麻醉后可能发生的意外情况及风险描述';
COMMENT ON COLUMN "麻醉知情同意书"."拟行有创操作和监测方法" IS '麻醉过程中拟实施的有创操作和监测的详细描述';
COMMENT ON COLUMN "麻醉知情同意书"."基础疾病对麻醉可能产生的影响" IS '患者所患的基础疾病可能对麻醉产生影响的特殊情况描述';
COMMENT ON COLUMN "麻醉知情同意书"."患者基础疾病" IS '患者所患的基础疾病的描述';
COMMENT ON COLUMN "麻醉知情同意书"."拟实施麻醉方法名称" IS '拟为患者进行手术、操作时使用的麻醉方法';
COMMENT ON COLUMN "麻醉知情同意书"."拟实施麻醉方法代码" IS '拟为患者进行手术、操作时使用的麻醉方法在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉知情同意书"."拟实施手术及操作时间" IS '拟对患者开始手术操作时的公元纪年日期和时间的完整描述';
COMMENT ON COLUMN "麻醉知情同意书"."拟实施麻醉方法" IS '拟为患者进行手术、操作时使用的麻醉方法';
COMMENT ON COLUMN "麻醉知情同意书"."拟实施手术及操作代码" IS '拟实施的手术及操作在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉知情同意书"."术前诊断名称" IS '术前诊断在特定编码体系中的名称。如存在多条诊断，是写主要诊断';
COMMENT ON COLUMN "麻醉知情同意书"."术前诊断代码" IS '术前诊断在特定编码体系中的代码';
COMMENT ON COLUMN "麻醉知情同意书"."年龄(小时)" IS '新出生婴儿实足年龄的小时数';
COMMENT ON COLUMN "麻醉知情同意书"."年龄(日)" IS '新出生婴儿实足年龄的天数';
COMMENT ON COLUMN "麻醉知情同意书"."年龄(月)" IS '儿童的实足年龄的月龄，以分数形式表示，分数的整数部分代表实足月龄，分数部分分母为30，分子为不足1个月的天数';
COMMENT ON COLUMN "麻醉知情同意书"."年龄(岁)" IS '患者年龄满1周岁的实足年龄，为患者后按照日历计算的历法年龄，以实足年龄的相应整数填写。1岁以内填0';
COMMENT ON COLUMN "麻醉知情同意书"."性别名称" IS '个体生理性别的标准名称，如男性、女性';
COMMENT ON COLUMN "麻醉知情同意书"."性别代码" IS '个体生理性别在特定编码体系中的代码';
CREATE TABLE IF NOT EXISTS "麻醉知情同意书" (
"患者姓名" varchar (50) DEFAULT NULL,
 "就诊次数" decimal (3,
 0) DEFAULT NULL,
 "病床号" varchar (10) DEFAULT NULL,
 "病房号" varchar (10) DEFAULT NULL,
 "病区名称" varchar (50) DEFAULT NULL,
 "科室代码" varchar (20) DEFAULT NULL,
 "科室名称" varchar (100) DEFAULT NULL,
 "知情同意书编号" varchar (20) DEFAULT NULL,
 "门诊就诊流水号" varchar (32) DEFAULT NULL,
 "住院就诊流水号" varchar (32) DEFAULT NULL,
 "就诊事件类型代码" varchar (2) DEFAULT NULL,
 "卡号" varchar (64) DEFAULT NULL,
 "卡类型代码" varchar (16) DEFAULT NULL,
 "患者唯一标识号" varchar (32) DEFAULT NULL,
 "修改标志" varchar (1) DEFAULT NULL,
 "麻醉知情同意书流水号" varchar (64) NOT NULL,
 "医疗机构名称" varchar (70) DEFAULT NULL,
 "医疗机构代码" varchar (22) NOT NULL,
 "数据更新时间" timestamp DEFAULT NULL,
 "数据采集时间" timestamp DEFAULT NULL,
 "密级" varchar (16) DEFAULT NULL,
 "麻醉医师签名时间" timestamp DEFAULT NULL,
 "麻醉医师姓名" varchar (50) DEFAULT NULL,
 "麻醉医师工号" varchar (20) DEFAULT NULL,
 "患者/法定代理人签名时间" timestamp DEFAULT NULL,
 "法定代理人与患者的关系名称" varchar (100) DEFAULT NULL,
 "法定代理人与患者的关系代码" varchar (1) DEFAULT NULL,
 "法定代理人姓名" varchar (50) DEFAULT NULL,
 "患者签名" varchar (50) DEFAULT NULL,
 "患者/法定代理人意见" text,
 "医疗机构意见" text,
 "参加麻醉安全保险标志" varchar (1) DEFAULT NULL,
 "使用麻醉镇痛泵标志" varchar (1) DEFAULT NULL,
 "麻醉中、麻醉后可能发生的意外及并发症" text,
 "拟行有创操作和监测方法" text,
 "基础疾病对麻醉可能产生的影响" text,
 "患者基础疾病" varchar (500) DEFAULT NULL,
 "拟实施麻醉方法名称" varchar (1000) DEFAULT NULL,
 "拟实施麻醉方法代码" varchar (20) DEFAULT NULL,
 "拟实施手术及操作时间" timestamp DEFAULT NULL,
 "拟实施麻醉方法" varchar (1000) DEFAULT NULL,
 "拟实施手术及操作代码" varchar (50) DEFAULT NULL,
 "术前诊断名称" varchar (100) DEFAULT NULL,
 "术前诊断代码" varchar (64) DEFAULT NULL,
 "年龄(小时)" decimal (3,
 0) DEFAULT NULL,
 "年龄(日)" decimal (3,
 0) DEFAULT NULL,
 "年龄(月)" decimal (8,
 0) DEFAULT NULL,
 "年龄(岁)" decimal (3,
 0) DEFAULT NULL,
 "性别名称" varchar (20) DEFAULT NULL,
 "性别代码" varchar (2) DEFAULT NULL,
 CONSTRAINT "麻醉知情同意书"_"麻醉知情同意书流水号"_"医疗机构代码"_PK PRIMARY KEY ("麻醉知情同意书流水号",
 "医疗机构代码")
);


