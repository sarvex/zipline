from interface import implements
from datashape import istabular

from .core import (
    bind_expression_to_resources,
)
from zipline.pipeline.common import (
    SID_FIELD_NAME,
    TS_FIELD_NAME,
    EVENT_DATE_FIELD_NAME,
)
from zipline.pipeline.loaders.base import PipelineLoader
from zipline.pipeline.loaders.blaze.utils import load_raw_data
from zipline.pipeline.loaders.events import (
    EventsLoader,
    required_event_fields,
)


class BlazeEventsLoader(implements(PipelineLoader)):
    """An abstract pipeline loader for the events datasets that loads
    data from a blaze expression.

    Parameters
    ----------
    expr : Expr
        The expression representing the data to load.
    next_value_columns : dict[BoundColumn -> raw column name]
        A dict mapping 'next' BoundColumns to their column names in `expr`.
    previous_value_columns : dict[BoundColumn -> raw column name]
        A dict mapping 'previous' BoundColumns to their column names in `expr`.
    resources : dict, optional
        Mapping from the loadable terms of ``expr`` to actual data resources.
    odo_kwargs : dict, optional
        Extra keyword arguments to pass to odo when executing the expression.

    Notes
    -----
    The expression should have a tabular dshape of::

       Dim * {{
           {SID_FIELD_NAME}: int64,
           {TS_FIELD_NAME}: datetime,
           {EVENT_DATE_FIELD_NAME}: datetime,
       }}

    And other dataset-specific fields, where each row of the table is a
    record including the sid to identify the company, the timestamp where we
    learned about the announcement, and the event date.

    If the '{TS_FIELD_NAME}' field is not included it is assumed that we
    start the backtest with knowledge of all announcements.
    """

    __doc__ = __doc__.format(SID_FIELD_NAME=SID_FIELD_NAME,
                             TS_FIELD_NAME=TS_FIELD_NAME,
                             EVENT_DATE_FIELD_NAME=EVENT_DATE_FIELD_NAME)

    def __init__(self,
                 expr,
                 next_value_columns,
                 previous_value_columns,
                 resources=None,
                 odo_kwargs=None):

        dshape = expr.dshape
        if not istabular(dshape):
            raise ValueError(f'expression dshape must be tabular, got: {dshape}')

        required_cols = list(
            required_event_fields(next_value_columns, previous_value_columns)
        )
        self._expr = bind_expression_to_resources(
            expr[required_cols],
            resources,
        )
        self._next_value_columns = next_value_columns
        self._previous_value_columns = previous_value_columns
        self._odo_kwargs = odo_kwargs if odo_kwargs is not None else {}

    def load_adjusted_array(self, domain, columns, dates, sids, mask):
        raw = load_raw_data(
            sids,
            domain.data_query_cutoff_for_sessions(dates),
            self._expr,
            self._odo_kwargs,
        )

        return EventsLoader(
            events=raw,
            next_value_columns=self._next_value_columns,
            previous_value_columns=self._previous_value_columns,
        ).load_adjusted_array(
            domain,
            columns,
            dates,
            sids,
            mask,
        )
