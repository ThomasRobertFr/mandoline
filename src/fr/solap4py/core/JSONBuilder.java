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
    private JSONBuilder() {
    }

    static JSONArray createJSONResponse(CellSet cellSet) throws OlapException, JSONException {
        JSONArray results = new JSONArray();
        boolean hasRows = false;

        if (cellSet.getAxes().size() > 1) {
            hasRows = true;
        }

        for (Position axis0 : cellSet.getAxes().get(Axis.COLUMNS.axisOrdinal()).getPositions()) {
            if (hasRows) {
                for (Position axis1 : cellSet.getAxes().get(Axis.ROWS.axisOrdinal()).getPositions()) {
                    final Cell cell = cellSet.getCell(axis0, axis1);
                    JSONObject result = new JSONObject();

                    for (Member member : axis1.getMembers()) {
                        result.put(member.getDimension().getUniqueName(), member.getUniqueName());
                    }

                    for (Member member : axis0.getMembers()) {
                        if (cell.getValue() == null) {
                            result.put(member.getUniqueName(), 0);
                        } else {
                            result.put(member.getUniqueName(), cell.getValue());
                        }
                    }
                    results.put(result);
                }
            } else {
                final Cell cell = cellSet.getCell(axis0);
                JSONObject result = new JSONObject();

                for (Member member : axis0.getMembers()) {
                    if (cell.getValue() == null) {
                        result.put(member.getUniqueName(), 0);
                    } else {
                        result.put(member.getUniqueName(), cell.getValue());
                    }
                }

                results.put(result);
            }
        }
        return results;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Solap4py s = Solap4py.getSolap4Object();

        String query = "{\"queryType\":\"data\",\"data\":{\"from\":\"[Sales]\",\"onColumns\":[\"[Measures].[Unit Sales]\",\"[Measures].[Store Cost]\"],\"onRows\":{\"[Time].[Year]\":{\"members\":[\"[Time].[Year].[1997]\",\"[Time].[Year].[1998]\"],\"range\":false},\"[Store].[Store Country]\":{\"members\":[\"[Store].[Store Country].[USA]\",\"[Store].[Store Country].[Canada]\"],\"range\":false}},\"where\":{}}}";

        System.out.println(s.process(query));

    }
}
