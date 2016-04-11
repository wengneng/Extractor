package com.pactera.edg.am.metamanager.extractor.adapter.extract.excel;

import java.io.*;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.*;
//import org.xml.sax.SAXException;





public class TestXSd {

    public static void main(String[] args) throws Exception, IOException {

        // 1. Lookup a factory for the W3C XML Schema language
        SchemaFactory factory = 
            SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
        
        // 2. Compile the schema. 
        // Here the schema is loaded from a java.io.File, but you could use 
        // a java.net.URL or a javax.xml.transform.Source instead.
        File schemaLocation = new File("E:\\autoXsd.xsd");
        Schema schema = factory.newSchema(schemaLocation);
    
        // 3. Get a validator from the schema.
        Validator validator = schema.newValidator();
        
        // 4. Parse the document you want to check.
        Source source = new StreamSource(new File("E:\\autoXml.xml"));
        
        // 5. Check the document
        try {
            validator.validate(source);
            System.out.println(" is valid.");

        }
        catch (Exception ex) {
            System.out.println( " is not valid because ");
            //System.out.println(ex.getMessage());
            ex.printStackTrace();
        }  
        
    }

}