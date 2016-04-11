package com.pactera.edg.am.metamanager.extractor.util;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Node;

import com.pactera.edg.am.metamanager.app.template.adapter.TemplateClsConfig;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateClsTitle;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateComp;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateDep;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateMdAttr;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitle;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitleJoint;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitleJointSet;

public class TemplateConfigReader
{
    public List<TemplateClsConfig> setupConfig(String path_config)
    {
        return setupConfig(new File(path_config));
    }
    
    public List<TemplateClsConfig> setupConfig(File configFile)
    {
        List <TemplateClsConfig> configList = new ArrayList<TemplateClsConfig>();
        try
        {
            Dom4jReader reader = new Dom4jReader();
            reader.initDocument(new FileInputStream(configFile));
            Document document = reader.getDocument();
            reader.close();
            List<Node> tcc = document.selectNodes(
                    "/template/templateClsConfigs/templateClsConfig");
            for (Node templateConfig: tcc)
            {            
                TemplateClsConfig config = parseClassTitle(templateConfig);
                configList.add(config);
                //设置子组合节点
            }
            
        } catch(Exception e)
        {
            
        }
        return configList;
    }

    private TemplateClsConfig parseClassTitle(Node templateConfig)
    {
        TemplateClsConfig config = new TemplateClsConfig();
        Node clsidNode = templateConfig.selectSingleNode("clsid");
        if(clsidNode != null)
        {
            config.setClsid(clsidNode.getStringValue());    
        }

        List<Node> classTitles = 
            templateConfig.selectNodes("clsTitles/templateClsTitle");

        for (Node classTitle: classTitles)
        {
            TemplateClsTitle clsTitle = new TemplateClsTitle();
            Node hrzNode = classTitle.selectSingleNode("hrz");
            if (hrzNode != null)
            {
                clsTitle.setHrz(Boolean.valueOf(
                        hrzNode.getStringValue()).booleanValue());
            }
            Node tstartNode = classTitle.selectSingleNode("tstart");
            if (tstartNode != null)
            {
                clsTitle.setTstart(Integer.valueOf(tstartNode.getStringValue()));
            }
            Node tendNode = classTitle.selectSingleNode("tend");
            if (tendNode != null)
            {
                clsTitle.setTend(Integer.valueOf(tendNode.getStringValue()));
            }
            Node dstartNode = classTitle.selectSingleNode("dstart");
            if (dstartNode != null)
            {
                clsTitle.setDstart(Integer.valueOf(dstartNode.getStringValue()));                
            }
            Node denttextNode = classTitle.selectSingleNode("dendtext");
            if(denttextNode != null)
            {
                clsTitle.setDendtext(denttextNode.getStringValue());                
            }

            //设置代码
            setUpCode(classTitle, clsTitle);

            //设置名称
            setUpName(classTitle, clsTitle);

            //设置属性
            setUpAttr(classTitle, clsTitle);
            
            config.addClsTitle(clsTitle);

            //设置组合关系
            setUpComp(classTitle, clsTitle);

            //设置依赖关系
            setUpDep(classTitle, clsTitle);

        }

        List<Node> compedConfigs = 
            templateConfig.selectNodes("compeds/templateClsConfig");

        if (compedConfigs.size() > 0)
        {
            for (Node compedConfig : compedConfigs)
            {
                config.addComped(parseClassTitle(compedConfig));
            }
            
        }
        return config;
    }

    private void setUpCode(Node classTitle, TemplateClsTitle clsTitle)
    {
        List<Node> codeTitleJoints = classTitle.selectNodes(
                "mdCode/titleJoints/templateTitleJoint");
        clsTitle.setMdCode(getJointSetByNodeList(codeTitleJoints));
    }

    private void setUpName(Node classTitle, TemplateClsTitle clsTitle)
    {
        List<Node> nameTitleJoints = classTitle.selectNodes(
                "mdName/titleJoints/templateTitleJoint");
        clsTitle.setMdName(getJointSetByNodeList(nameTitleJoints));
    }

    private void setUpAttr(Node classTitle, TemplateClsTitle clsTitle)
    {
        List<Node> metaAttrs = classTitle.selectNodes("mdAttrs/templateMdAttr");
        for (Node metaAttr: metaAttrs)
        {
            TemplateMdAttr mdAttr = new TemplateMdAttr();
            Node attCodeNode = metaAttr.selectSingleNode("attCode");
            if (attCodeNode != null)
            {
                mdAttr.setAttCode(attCodeNode.getStringValue());                
            }
            List<Node> attrTitleJoints = classTitle.selectNodes(
                    "titleJnts/titleJoints/templateTitleJoint");
            mdAttr.setTitleJnts(getJointSetByNodeList(attrTitleJoints));
            clsTitle.addMdAttr(mdAttr);
        }
    }

    private void setUpComp(Node classTitle, TemplateClsTitle clsTitle)
    {
        List<Node> comps = classTitle.selectNodes("comps/templateComp");
        for (Node comp: comps)
        {
            TemplateComp mdComp = new TemplateComp();
            Node compClsIdNode = comp.selectSingleNode("compClsId");
            if (compClsIdNode != null)
            {
                mdComp.setCompClsId(compClsIdNode.getStringValue());                
            }
            List<Node> compTitleJoints = comp.selectNodes(
                    "titleJnts/titleJoints/templateTitleJoint");
            mdComp.setTitleJnts(getJointSetByNodeList(compTitleJoints));
            clsTitle.addComp(mdComp);
        }
    }

    private void setUpDep(Node classTitle, TemplateClsTitle clsTitle)
    {
        List<Node> deps = classTitle.selectNodes("deps/templateDep");
        for (Node dep: deps)
        {
            TemplateDep mdDep = new TemplateDep();
            Node depedClsIdNode = dep.selectSingleNode("depedClsId");
            if (depedClsIdNode != null)
            {
                mdDep.setDepedClsId(depedClsIdNode.getStringValue());                
            }
            Node froleNode = dep.selectSingleNode("frole");
            Node troleNode = dep.selectSingleNode("trole");
            if (froleNode != null)
            {
                String froleValue = froleNode.getStringValue();                
                mdDep.setFrole(froleValue.length() == 0 ? null : froleValue);
            } else
            {
                mdDep.setFrole(null);
            }
            if (troleNode != null)
            {
                String troleValue = troleNode.getStringValue();
                mdDep.setTrole(troleValue.length() == 0 ? null : troleValue);                
            } else
            {
                mdDep.setTrole(null);
            }
            List<Node> relationComps = dep.selectNodes("relateConds/templateComp");
            for (Node relationComp: relationComps)
            {
                TemplateComp mdDepComp = new TemplateComp();
                Node compClsIdNode = relationComp.selectSingleNode("compClsId");
                if (compClsIdNode != null)
                {
                    mdDepComp.setCompClsId(compClsIdNode.getStringValue());                    
                }
                List<Node> depTitleJoints = relationComp.selectNodes(
                        "titleJnts/titleJoints/templateTitleJoint");
                mdDepComp.setTitleJnts(
                        getJointSetByNodeList(depTitleJoints));
                mdDep.addRelateCond(mdDepComp);
            }
            clsTitle.addDep(mdDep);
        }
    }

    private TemplateTitleJointSet getJointSetByNodeList(List<Node> titleJointNodes)
    {
        TemplateTitleJointSet depTitleJointSet = new TemplateTitleJointSet();
        for (Node depTitleJoint: titleJointNodes)
        {
            TemplateTitleJoint tplTitleJoint = new TemplateTitleJoint();
            Node joinWords = depTitleJoint.selectSingleNode("joinWords");
            if(joinWords != null)
            {
                tplTitleJoint.setJoinWords(joinWords.getStringValue());
            }
            
            Node titleDir = depTitleJoint.selectSingleNode("titleDirection");
            if (titleDir != null)
            {
                tplTitleJoint.setTitleDirection(titleDir.getStringValue());
            }
            List<Node> titles = depTitleJoint.selectNodes("titles/templateTitle");
            for (Node title: titles)
            {
                TemplateTitle tplTitle = new TemplateTitle();
                Node nameNode = title.selectSingleNode("name");
                if (nameNode != null)
                {
                    tplTitle.setName(nameNode.getStringValue());                    
                }
                Node mergeAcrsNode = title.selectSingleNode("mergeAcrs");
                if (mergeAcrsNode != null)
                {
                    tplTitle.setMergeDown(Integer.valueOf(
                            mergeAcrsNode.getStringValue()));                    
                }
                Node mergeDownNode = title.selectSingleNode("mergeDown");
                if (mergeDownNode != null)
                {
                    tplTitle.setMergeDown(Integer.valueOf(
                            mergeDownNode.getStringValue()));
                }
                
                Node tempIdNode = title.selectSingleNode("tempId");
                if (tempIdNode != null)
                {
//                    tplTitle.setTempId(tempIdNode.getStringValue());
                }
                tplTitleJoint.addTitle(tplTitle);
            }
            depTitleJointSet.addTitleJoint(tplTitleJoint);
        }
        return depTitleJointSet;
    }
}
