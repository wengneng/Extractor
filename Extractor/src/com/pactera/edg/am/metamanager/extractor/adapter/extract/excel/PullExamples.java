package com.pactera.edg.am.metamanager.extractor.adapter.extract.excel;

import com.saxonica.validate.SchemaAwareConfiguration;
import net.sf.saxon.Configuration;
import net.sf.saxon.TransformerFactoryImpl;
import net.sf.saxon.event.PipelineConfiguration;
import net.sf.saxon.event.Receiver;
import net.sf.saxon.event.SerializerFactory;
import net.sf.saxon.event.Sink;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.Validation;
import net.sf.saxon.pull.*;
import net.sf.saxon.query.DynamicQueryContext;
import net.sf.saxon.query.StaticQueryContext;
import net.sf.saxon.query.XQueryExpression;
import net.sf.saxon.tinytree.TinyBuilder;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.tree.TreeBuilder;
import net.sf.saxon.value.Value;
import net.sf.saxon.value.Whitespace;

import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.util.Properties;
import java.text.SimpleDateFormat;
import java.util.*;
/**
 * This class contains some examples of how to use the Pull interfaces
 * in Saxon.
 */

public class PullExamples {

    private Configuration config;

    /**
     * Serialize a document that is supplied via the pull interface
     */

    public void serialize(PullProvider in, OutputStream out) throws XPathException, IOException {
        Properties props = new Properties();
        props.setProperty(OutputKeys.METHOD, "xml");
        props.setProperty(OutputKeys.INDENT, "yes");
        SerializerFactory sf = config.getSerializerFactory();
        Receiver receiver = sf.getReceiver(new StreamResult(out),
                                                        in.getPipelineConfiguration(),
                                                        props);
        new PullPushCopier(in, receiver).copy();
    }

    /**
     * Validate a document that is supplied via the pull interface
     * (This requires the schema-aware version of Saxon)
     */

    public void validate(PullProvider in) throws XPathException, IOException {
        SchemaAwareConfiguration config = new SchemaAwareConfiguration();
        in.getPipelineConfiguration().setConfiguration(config);
        in.getPipelineConfiguration().setSchemaURIResolver(config.getSchemaURIResolver());
        Receiver sink = new Sink();
        sink.setPipelineConfiguration(in.getPipelineConfiguration());
        Receiver validator = config.getDocumentValidator(
                sink, in.getSourceLocator().getSystemId(), Validation.STRICT, Whitespace.NONE, null, -1);
        validator.setPipelineConfiguration(in.getPipelineConfiguration());
        //in = new PullTracer(in);
        new PullPushCopier(in, validator).copy();
        System.out.println("Done.");
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
     * Run a query against input that is supplied using the pull interface
     */

    public void query(PullProvider in, String query, OutputStream out) throws XPathException {
        final StaticQueryContext sqc = new StaticQueryContext(config);
        final XQueryExpression exp = sqc.compileQuery(query);
        final DynamicQueryContext dynamicContext = new DynamicQueryContext(config);
        dynamicContext.setContextItem(config.buildDocument(new PullSource(in)));
        Properties props = new Properties();
        props.setProperty(OutputKeys.INDENT, "yes");
        exp.run(dynamicContext, new StreamResult(out), props);
    }

    /**
     * Build a Saxon "tiny" tree from input supplied via the pull interface
     */

    public NodeInfo build(PullProvider in) throws XPathException {
        TinyBuilder builder = new TinyBuilder();
        builder.setPipelineConfiguration(in.getPipelineConfiguration());
        new PullPushCopier(in, builder).copy();
        return builder.getCurrentRoot();
    }

    /**
     * Build a Saxon "standard" tree from input supplied via the pull interface
     */

    public NodeInfo buildStandardTree(PullProvider in) throws XPathException {
        TreeBuilder builder = new TreeBuilder();
        builder.setPipelineConfiguration(in.getPipelineConfiguration());
        builder.open();
        new PullPushCopier(in, builder).copy();
        builder.close();
        return builder.getCurrentRoot();
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

    /**
     * Get a PullProvider based on a document or element node in a Saxon tree
     */

    public PullProvider getTreeWalker(NodeInfo root) {
        return TreeWalker.makeTreeWalker(root);
    }

    /**
     * Run a query to produce a sequence of element nodes, and get a PullProvider over the results
     * of the query
     */

    public PullProvider pullQueryResults(NodeInfo source, String query) throws XPathException {
        final StaticQueryContext sqc = new StaticQueryContext(config);
        final XQueryExpression exp = sqc.compileQuery(query);
        final DynamicQueryContext dynamicContext = new DynamicQueryContext(config);
        dynamicContext.setContextItem(source);
        PullProvider pull = new PullFromIterator(exp.iterator(dynamicContext));
        pull = new PullNamespaceReducer(pull);
        pull.setPipelineConfiguration(config.makePipelineConfiguration());
        return pull;
    }

    /**
     * Create a copy of a document, filtered to remove all elements named "PRICE",
     * together with their contents
     */

    public void removePriceElements(PullProvider in, OutputStream out) throws IOException, XPathException {
        final int priceElement = config.getNamePool().allocate("", "", "PRICE");
        PullFilter filter = new PullFilter(in) {
            public int next() throws XPathException {
                currentEvent = super.next();
                if (currentEvent == START_ELEMENT && getFingerprint() == priceElement) {
                    super.skipToMatchingEnd();
                    currentEvent = next();
                }
                return currentEvent;
            }
        };
        serialize(filter, out);
    }

    /**
     * Get the average price of books, by scanning a document and taking the average of the
     * values of the PRICE elements
     */

     public void displayAveragePrice(PullProvider in, OutputStream out) throws IOException, XPathException {
        final int priceElement = config.getNamePool().allocate("", "", "PRICE");
        double total = 0;
        int count = 0;
        while (true) {
            int event = in.next();
            if (event == PullProvider.END_OF_INPUT) {
                break;
            }
            if (event == PullProvider.START_ELEMENT && in.getFingerprint() == priceElement) {
                double value = Value.stringToNumber(in.getStringValue());
                total += value;
                count++;
            }
        }
        double average = (count==0 ? Double.NaN : total/count);
        String result = "<result>" + average + "</result>";
        OutputStreamWriter writer = new OutputStreamWriter(out);
        writer.write(result);
        writer.flush();
    }

    public void test() throws Exception{
    	File stylesheet = null;
    	File input = null;
    	OutputStream output = null;
    	output = System.out;
    	input = new File("E:\\workspace\\MM_my\\xslt\\test09.xml");
    	stylesheet = new File("E:\\workspace\\MM_my\\xslt\\test08.xslt");
    	
        PullExamples o = new PullExamples();
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
     * Main program. Arguments:
     * <ol>
     *   <li>Select examples. Each example is identified by a single letter. Use # to run all examples.
     *   <li>-s Source document</li>
     *   <li>-o Output file</li>
     *   <li>-xsl Stylesheet</li>
     *   <li>-q Query
     * </ol>
     */

    public static void main(String[] args) throws Exception {
    	PullExamples pe = new PullExamples();
    	pe.test();
    	//test();
    	/*
        String examples;
        if (args.length < 1 || args[0].equals("#")) {
            examples = "abcdefghijklmnopqrstuvwxyz";
        } else {
            examples = args[0];
        }
        File input = null;
        OutputStream output = null;
        String query = null;
        File stylesheet = null;

        for (int i=1; i<args.length; i++) {
            if (args[i].equals("-s")) {
                input = new File(args[++i]);
            } else if (args[i].equals("-o")) {
                output = new FileOutputStream(new File(args[++i]));
            } else if (args[i].equals("-q")) {
                query = readFile(new File(args[++i]));
            } else if (args[i].equals("-xsl")) {
                stylesheet = new File(args[++i]);
            } else {
                System.err.println("Unknown argument " + args[i]);
            }
        }

        if (input==null) {
            input = new File("data/books.xml");
        }

        if (output==null) {
            output = System.out;
        }

        if (stylesheet==null) {
            stylesheet = new File("styles/books.xsl");
        }

        if (query==null) {
            query = readFile(new File("query/books.xq"));
        }

        PullExamples o = new PullExamples();
        Configuration config = new Configuration();
        config.setLazyConstructionMode(true);
        //config.setLineNumbering(true);
        o.config = config;

        PipelineConfiguration pipe = config.makePipelineConfiguration();
        for (int i=0; i<examples.length(); i++) {
            char ex = examples.charAt(i);
            switch (ex) {
                case 'a': {
                    System.out.println("\n\n=== Serialize the input to the output ===\n");

                    PullProvider p = o.getParser(input);
                    p.setPipelineConfiguration(pipe);
                    o.serialize(p, output);
                    break;
                }
                case 'b': {
                    System.out.println("\n\n=== Validate the input ===\n");

                    PullProvider p = o.getParser(input);
                    p.setPipelineConfiguration(pipe);
                    o.validate(p);
                    break;
                }
                case 'c': {
                    System.out.println("\n\n=== Transform the input to the output ===\n");

                    if (stylesheet == null) {
                        System.err.println("** No stylesheet supplied");
                        break;
                    }
                    PullProvider p = o.getParser(input);
                    p.setPipelineConfiguration(pipe);
                    o.transform(p, stylesheet, output);
                    break;
                }
                case 'd': {
                    System.out.println("\n\n=== Run XQuery against the input ===\n");
                    if (query == null) {
                        query = "<result>{.}</result>";
                    }
                    PullProvider p = o.getParser(input);
                    p.setPipelineConfiguration(pipe);
                    o.query(p, query, output);
                    break;
                }
                case 'e': {
                    System.out.println("\n\n=== Remove PRICE elements from the input ===\n");

                    PullProvider p = o.getParser(input);
                    p.setPipelineConfiguration(pipe);
                    o.removePriceElements(p, output);
                    break;
                }
                case 'f': {
                    System.out.println("\n\n=== Compute average of PRICE elements in the input ===\n");

                    PullProvider p = o.getParser(input);
                    p.setPipelineConfiguration(pipe);
                    o.displayAveragePrice(p, output);
                    break;
                }
                case 'g': {
                    System.out.println("\n\n=== Obtain query results using a pull iterator ===\n");

                    NodeInfo node = config.buildDocument(new StreamSource(input));
                    PullProvider p = o.pullQueryResults(node,

                            "declare function local:f() {"+
                              "for $var1 in (<abc/>, <def/>)"+
                              "return <e xmlns:x='x1'><f xmlns:y='y1' xmlns:x='x2'>xyz</f></e>};"+
                            "local:f()"

                    );
                    o.serialize(new PullTracer(p), output);
                    break;
                }
                case 'h': {
                    System.out.println("\n\n=== Obtain query results using a pull iterator on a 'standard' tree ===\n");

                    PullProvider p1 = o.getParser(input);
                    p1.setPipelineConfiguration(pipe);
                    NodeInfo node = o.buildStandardTree(p1);
                    PullProvider p2 = o.pullQueryResults(node,
                           "//CATEGORIES"
                    );
                    o.serialize(p2, output);
                }
            }
        }
        */
    }

    /**
     * Read the contents of a file into a string
     */

    public static String readFile(File file) throws IOException {
        Reader reader = new FileReader(file);
        char[] buffer = new char[4096];
        StringBuffer sb = new StringBuffer(4096);
        while (true) {
            int n = reader.read(buffer);
            if (n>0) {
                sb.append(buffer, 0, n);
            } else {
                break;
            }
        }
        return sb.toString();
    }



}

//
// The contents of this file are subject to the Mozilla Public License Version 1.0 (the "License");
// you may not use this file except in compliance with the License. You may obtain a copy of the
// License at http://www.mozilla.org/MPL/
//
// Software distributed under the License is distributed on an "AS IS" basis,
// WITHOUT WARRANTY OF ANY KIND, either express or implied.
// See the License for the specific language governing rights and limitations under the License.
//
// The Original Code is: all this file.
//
// The Initial Developer of the Original Code is Michael H. Kay.
//
// Portions created by (your name) are Copyright (C) (your legal entity). All Rights Reserved.
//
// Contributor(s): none.
//
