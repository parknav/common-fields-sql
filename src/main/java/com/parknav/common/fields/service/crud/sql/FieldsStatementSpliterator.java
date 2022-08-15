package com.parknav.common.fields.service.crud.sql;

import com.parknav.common.sql.StatementSpliterator;
import com.steatoda.nar.NarField;
import com.steatoda.nar.NarGraph;
import com.steatoda.nar.NarObject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FieldsStatementSpliterator<C extends NarObject<C, F>, F extends Enum<F> & NarField> extends StatementSpliterator<C> {

	@FunctionalInterface
	public interface FieldsResolver<C extends NarObject<C, F>, F extends Enum<F> & NarField> {
		C resolve(ResultSet resultSet, NarGraph<F> graph) throws SQLException;
	}

	public FieldsStatementSpliterator(PreparedStatement statement, FieldsResolver<C, F> fieldsResolver, NarGraph<F> graph) {
		super(statement, resultSet -> fieldsResolver.resolve(resultSet, graph));
	}

}
