package fr.solap4py.core;

import java.sql.SQLException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.olap4j.Axis;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.metadata.Member;

final class JSONBuilder {
    private JSONBuilder() {}
    
    static JSONArray createJSONResponse(CellSet cellSet) throws OlapException, JSONException {
        JSONArray results = new JSONArray();
        for (Position axis0 : cellSet.getAxes().get(Axis.ROWS.axisOrdinal()).getPositions()) {
            for (Position axis1 : cellSet.getAxes().get(Axis.COLUMNS.axisOrdinal()).getPositions()) {
                final Cell cell = cellSet.getCell(axis1, axis0);
                JSONObject result = new JSONObject();
                
                for (Member member : axis0.getMembers()) {                    
                    result.append(member.getDimension().getUniqueName(), member.getUniqueName());
                }
                
                for (Member member : axis1.getMembers()) {
                    result.append(member.getUniqueName(), cell.getValue());
                }
                results.put(result);
            }
        }
        
        return results;
    }
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String s = "{\"queryType\": \"data\" , \"data\" : {" + "\"onColumns\":" + "[" + "\"[Measures].[Goods Quantity]\"," + "\"[Measures].[Max Quantity]\"" + "]," + " \"onRows\":" + "{" + "\"[Time]\":{\"members\":[\"[2000]\", \"[2010]\"],\"range\":\"false\"}, " + "\"[Zone.Name]\":{\"members\":[\"[France]\"],\"range\":\"false\"} " + "}," + "\"from\":" + "\"[Traffic]\"" + "}}";
        System.out.println(s);
        Solap4py sp = Solap4py.getSolap4Object();
        String result = sp.process(s);
        System.out.println(result);
        
        
    }
}
