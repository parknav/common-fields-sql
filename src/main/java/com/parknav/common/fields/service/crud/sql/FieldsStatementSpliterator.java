package com.parknav.common.fields.service.crud.sql;

import com.parknav.common.fields.FieldEnum;
import com.parknav.common.fields.FieldGraph;
import com.parknav.common.fields.HasFields;
import com.parknav.common.sql.StatementSpliterator;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FieldsStatementSpliterator<C extends HasFields<C, F>, F extends Enum<F> & FieldEnum> extends StatementSpliterator<C> {

	@FunctionalInterface
	public interface FieldsResolver<C extends HasFields<C, F>, F extends Enum<F> & FieldEnum> {
		C resolve(ResultSet resultSet, FieldGraph<F> graph) throws SQLException;
	}

	public FieldsStatementSpliterator(PreparedStatement statement, FieldsResolver<C, F> fieldsResolver, FieldGraph<F> graph) {
		super(statement, resultSet -> fieldsResolver.resolve(resultSet, graph));
	}

}
