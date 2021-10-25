using System.Collections.Concurrent;
using System.Runtime.Loader;

namespace revtask.Core;

public abstract class GenericCollection<TConstraints, TCollection> : IDisposable
{
    protected ConcurrentDictionary<Type, TCollection> Dictionary = new();
    
    private List<Action> _unloadMethodList = new();
    protected TCollection GetCollection<T>()
        where T : TConstraints
    {
        var type = typeof(T);
        if (!Dictionary.TryGetValue(type, out var collection))
        {
            Dictionary[type] = collection = OnCollectionAdded<T>();

            void Remove(AssemblyLoadContext _) => RemoveCollection<T>();

            AssemblyLoadContext.GetLoadContext(type.Assembly)!
                .Unloading += Remove;

            void Unload()
            {
                AssemblyLoadContext.GetLoadContext(typeof(T).Assembly)!
                    .Unloading -= Remove;

                lock (_unloadMethodList)
                {
                    _unloadMethodList.Remove(Unload);
                }
            }

            lock (_unloadMethodList)
            {
                _unloadMethodList.Add(Unload);
            }
        }

        return collection;
    }

    protected bool RemoveCollection<T>()
        where T : TConstraints
    {
        if (!Dictionary.TryGetValue(typeof(T), out var collection))
            return false;

        OnCollectionRemoved<T>(collection);
        Dictionary.Remove(typeof(T), out _);
        return true;
    }

    public virtual void Clear()
    {
        Dictionary.Clear();
        lock (_unloadMethodList)
        {
            foreach (var method in _unloadMethodList.ToArray())
                method();

            _unloadMethodList.Clear();
        }
    }

    public void Dispose()
    {
        Clear();
    }

    protected abstract TCollection OnCollectionAdded<T>() where T : TConstraints;

    protected virtual void OnCollectionRemoved<T>(TCollection collection)
        where T : TConstraints
    {
    }
}