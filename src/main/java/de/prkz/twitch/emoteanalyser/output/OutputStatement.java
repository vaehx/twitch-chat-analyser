package de.prkz.twitch.emoteanalyser.output;

import java.util.ArrayList;
import java.util.List;

@Deprecated
public class OutputStatement {
    private String sql;

    public static OutputStatement of(String sql) {
        OutputStatement stmt = new OutputStatement();
        stmt.sql = sql;
        return stmt;
    }

    public String getSql() {
        return sql;
    }

    public static BatchBuilder buildBatch() {
        return new BatchBuilder();
    }

    public static class BatchBuilder {
        private List<OutputStatement> statements = new ArrayList<>();

        public BatchBuilder add(String sql) {
            statements.add(OutputStatement.of(sql));
            return this;
        }

        public List<OutputStatement> finish() {
            return statements;
        }
    }
}
