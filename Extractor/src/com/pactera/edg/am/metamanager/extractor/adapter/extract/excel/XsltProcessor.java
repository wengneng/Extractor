package com.pactera.edg.am.metamanager.extractor.adapter.extract.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.Configuration;
import net.sf.saxon.TransformerFactoryImpl;
import net.sf.saxon.event.PipelineConfiguration;
import net.sf.saxon.pull.PullProvider;
import net.sf.saxon.pull.PullSource;
import net.sf.saxon.pull.StaxBridge;
import net.sf.saxon.trans.XPathException;

public class XsltProcessor {
	private Configuration config;
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		XsltProcessor o = new XsltProcessor();
		o.test();
		// TODO Auto-generated method stub

	}
	
    public void test() throws Exception{
    	File stylesheet = null;
    	File input = null;
    	OutputStream output = null;
    	output = System.out;
    	input = new File("E:\\workspace\\MM_my\\xslt\\test09.xml");
    	stylesheet = new File("E:\\workspace\\MM_my\\xslt\\test08.xslt");
    	
    	XsltProcessor o = new XsltProcessor();
        Configuration config = new Configuration();
        config.setLazyConstructionMode(true);
        //config.setLineNumbering(true);
        o.config = config;

        PipelineConfiguration pipe = config.makePipelineConfiguration();    	
        System.out.println("\n\n=== Transform the input to the output ===\n");
        
        PullProvider p = o.getParser(input);
        p.setPipelineConfiguration(pipe);
         
		Date d = new Date();
		long longtime = d.getTime();
		//你获得的是上面的long型数据吧

		//也可以自己用SimpleDateFormat这个函数把它变成自己想要的格式,注意需要import java.text.SimpleDateFormat;
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"); 
		System.out.println(sdf.format(longtime));
              

        o.transform(p, stylesheet, output);
        longtime = d.getTime();
        System.out.println(sdf.format(longtime));
    }

    /**
     * Transform a document supplied via the pull interface
     */

    public void transform(PullProvider in, File stylesheet, OutputStream out) throws TransformerException {
        TransformerFactory factory = new TransformerFactoryImpl();
        Templates templates = factory.newTemplates(new StreamSource(stylesheet));
        Transformer transformer = templates.newTransformer();
        transformer.transform(
                new PullSource(in),
                new StreamResult(out)
        );
    }
    /**
     * Get a PullProvider based on a StAX Parser for a given input file
     */

    public PullProvider getParser(File input) throws FileNotFoundException, XPathException {
        StaxBridge parser = new StaxBridge();
        parser.setInputStream(input.toURI().toString(), new FileInputStream(input));
        parser.setPipelineConfiguration(config.makePipelineConfiguration());
        // For diagnostics, confirm which XMLStreamReader implementation is being used
        //System.err.println("StAX parser: " + parser.getXMLStreamReader().getClass().getName());
        return parser;
    }
}
