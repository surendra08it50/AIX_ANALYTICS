import copy

from sqlalchemy import exc
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm.session import Session

from dashboard_analytics.db.db_connection_util import DbConnectionUtil
from dashboard_analytics.exceptions.db import DbException


class ServiceBase:
    def __init__(self, model):
        self.model = model
        self.session: Session = DbConnectionUtil().get_db_session_scope()

    def find_one(self, findOneOptions):

        try:
            results = self.session.query(self.model).filter(findOneOptions).first()
            return results
        except exc.SQLAlchemyError as e:
            self.session.rollback()
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def find_all(self):

        try:
            results = self.session.query(self.model).all()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def find_many(self, findManyOptions, sort_by_key=None, fields="*"):
        from sqlalchemy.orm import load_only

        try:
            results = (
                self.session.query(self.model)
                .filter(findManyOptions)
                .options(load_only(*fields))
                .order_by(sort_by_key)
                .all()
            )
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def create_one(self, obj):

        try:
            self.session.add(obj)
            self.session.flush()
            createdObj = copy.deepcopy(obj)
            self.session.commit()
            return createdObj
        except exc.SQLAlchemyError as e:
            self.session.rollback()
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def create_many(self, obj):
        try:
            self.session.add_all(obj)
            self.session.commit()
        except exc.SQLAlchemyError as e:
            self.session.rollback()
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def update_one(self, obj, id):

        try:
            results = self.session.query(self.model).get(id)
            if results is None:
                raise DbException("No records Found")
            self.session.merge(obj)
            self.session.commit()
            return obj
        except exc.SQLAlchemyError as e:
            self.session.rollback()
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def update_one_by_attr(self, obj, find_option):

        try:
            items_tobe_update = {
                key: value for key, value in obj.getvals().items() if value is not None
            }
            results = (
                self.session.query(self.model)
                .filter(find_option)
                .update(items_tobe_update)
            )
            self.session.commit()

            # TODO : not found single call to return updated record object.
            # so, for now we are doing temporary patching with two call.
            result = self.session.query(self.model).filter(find_option).first()
            if result is None:
                raise DbException("No records Found")
            return result
        except exc.SQLAlchemyError as e:
            self.session.rollback()
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def update_many(self, obj):

        try:
            self.session.bulk_update_mappings(self.model, obj)
            self.session.commit()
        except exc.SQLAlchemyError as e:
            self.session.rollback()
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def upsert_one(self, model, uniq, obj):

        try:
            stmt = insert(self.model).values(**obj)
            stmt = stmt.on_conflict_do_update(index_elements=[uniq], set_=obj)
            self.session.execute(stmt)
            self.session.commit()
            return obj
        except exc.SQLAlchemyError as e:
            self.session.rollback()
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def delete_one(self, id):

        try:
            results = self.session.query(self.model).get(id)
            self.session.delete(results)
            self.session.commit()
        except exc.SQLAlchemyError as e:
            self.session.rollback()
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def delete_many(self, deleteManyOptions):

        try:
            results = (
                self.session.query(self.model)
                .filter(deleteManyOptions)
                .delete(synchronize_session="fetch")
            )
            self.session.commit()
        except exc.SQLAlchemyError as e:
            self.session.rollback()
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def find_query_data(self, fields, sort_by_key=None):
        from sqlalchemy.orm import load_only

        try:
            results = (
                self.session.query(self.model)
                .options(load_only(*fields))
                .order_by(sort_by_key)
                .all()
            )
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def find_query_data_onselect(self, reqjson, fields, sort_by_key=None):
        from sqlalchemy.orm import load_only

        results = None
        try:
            results = (
                self.session.query(self.model)
                .filter_by(**reqjson)
                .options(load_only(*fields))
                .order_by(sort_by_key)
                .all()
            )
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()
