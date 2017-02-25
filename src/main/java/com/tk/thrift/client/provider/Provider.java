package com.tk.thrift.client.provider;

import java.util.Collection;

/**
 * @author Tingkun Zhang
 */
public interface Provider<T> {

    Collection<T> list();

    void addListener(ProviderListener<T> listener);

}
