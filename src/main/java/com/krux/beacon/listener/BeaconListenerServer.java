package com.krux.beacon.listener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.stdlib.KruxStdLib;

public class BeaconListenerServer {
    
    private static final Logger log = LoggerFactory.getLogger(BeaconListenerServer.class.getName());
    
    public static Map<Integer, List<String>> portToTopicsMap = new HashMap<Integer, List<String>>();
    public static List<Thread> servers = new ArrayList<Thread>();

    public static void main(String[] args) {
        
     // handle a couple custom cli-params
        OptionParser parser = new OptionParser();

        OptionSpec<String> portTopicMappings = parser
                .accepts("portTopic", "The port->topic mappings")
                .withOptionalArg()
                .ofType(String.class);
        
        OptionSpec<String> option2 = parser
                .accepts("option2", "The port->topic mappings")
                .withOptionalArg()
                .ofType(String.class);

        // give parser to KruxStdLib so it can add our params to the reserved
        // list
        KruxStdLib.setOptionParser(parser);
        OptionSet options = KruxStdLib.initialize(args);
        
        Map<OptionSpec<?>,List<?>> optionMap = options.asMap();
        List<?> portTopicMap = optionMap.get( portTopicMappings );
        
        for ( Object mapping : portTopicMap ) {
            String mappingString = (String) mapping;
            String[] parts = mappingString.split(":");
            Integer port = Integer.parseInt(parts[0]);
            String[] topics = parts[1].split(",");
            
            List<String> topicList = new ArrayList<String>();
            System.out.println( "port: " + parts[0] );
            for ( String topic : topics ) {
                System.out.println( "  " + topic );
                topicList.add( topic );
            }
            portToTopicsMap.put( port, topicList );
        }
        
        //ok, mappings populated. Now, start tcp server on each port
        for ( Map.Entry<Integer, List<String>> entry : portToTopicsMap.entrySet() ) {
            BeaconListener listener = new BeaconListener( entry.getKey(), entry.getValue() );
            Thread t = new Thread( listener );
            servers.add( t );
            t.start();
        }
        
        System.out.println( "Done." );

    }

}
