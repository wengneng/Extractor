package com.pactera.edg.am.metamanager.extractor.bo.mm;

import java.util.ArrayList;
import java.util.List;

public class TemplateMetadata extends MMMetadata
{

    private static final long serialVersionUID = -6007693470410013919L;
    
    private List<TemplateMetadata> children = new ArrayList<TemplateMetadata>();

    public List<TemplateMetadata> getChildren()
    {
        return children;
    }

    public void setChildren(List<TemplateMetadata> children)
    {
        this.children = children;
    } 

    public void addChild(TemplateMetadata child)
    {
        this.children.add(child);
    }
}
