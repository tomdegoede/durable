/*
 * This POC uses C# async statemachine logic to create durable functions that can be resumed after a certain time,
 * even when the process has terminated in between. So-called durable functions have gained traction lately.
 * Examples include:
 * - https://temporal.io/blog/workflow-introduction
 * - https://learn.microsoft.com/en-us/azure/azure-functions/durable/
 * - https://www.golem.cloud/
 * - https://blog.cloudflare.com/building-workflows-durable-execution-on-workers/
 *
 * Most solutions still require some boilerplate by defining deterministic actions and using replaying to reconstruct state.
 *
 * I believe that the solution demonstrated in this file is very small, efficient and allows for easy embedding in .NET applications.
 */

using System.Runtime.CompilerServices;
using MessagePack;
using MessagePack.Formatters;
using MessagePack.Resolvers;

var tasksdir = new DirectoryInfo("tasks");
tasksdir.Create(); // if not exists

// Reconstruct the tasks that were persisted in a previous execution
var durable = Reconstruct(tasksdir.EnumerateFiles().Select(f => File.ReadAllBytes(f.FullName)));
// Pretend that our Durable dependency WaitForOneDay finished.
durable.SetResult(); // this will print our output!

DurableTask WaitForOneDay() => new(canComplete: false);

async DurableTask Main1337()
{
    int[] state = [1, 3, 3, 7];
    await WaitForOneDay();
    Console.WriteLine(string.Join("", state));
}

async DurableTask Main()
{
    int[] state = [1];
    await Main1337();
    Console.WriteLine(string.Join("", state));
}

// Execute main without await. This will popuplate the DurableStack
Main();

// persist our stack to prove we are durable. We execute this in a next invocation.
for (var i = 0; i < DurableStack.States.Count; i++)
{
    File.WriteAllBytes($"tasks/{i}.sm", DurableStack.States[i]);
}

// Run our stack immediately as if fully completed. This will also print our output
Reconstruct(DurableStack.States).SetResult();

DurableAwaiter Reconstruct(IEnumerable<byte[]> stateStack)
{
    var mostInnerAwaiter = new DurableAwaiter();
    var innerAwaiter = mostInnerAwaiter;
    foreach (var stateBytes in stateStack)
    {
        var (awaiter, sm) = ReconstructStatemachine(stateBytes);
        innerAwaiter.OnCompleted(sm.MoveNext);
        innerAwaiter = awaiter;
    }
    return mostInnerAwaiter;
}

(DurableAwaiter, IAsyncStateMachine) ReconstructStatemachine(byte[] statemachineBytes)
{
    // Track all creater DurableAwaiters
    var formatterResolver = new DurableAwaiterResolver();
    var options = MessagePackSerializer.Typeless.DefaultOptions.WithResolver(
        CompositeResolver.Create(formatterResolver, MessagePackSerializer.Typeless.DefaultOptions.Resolver));

    var asyncStateMachine = (IAsyncStateMachine)MessagePackSerializer.Typeless.Deserialize(statemachineBytes, options);
    
    // return the statemachines own awaiter to hook up outer call
    // we do not care about awaiters[1..] as they represent inner tasks that are already reconstructed
    return (formatterResolver._awaiters[0], asyncStateMachine);
}

class DurableAwaiterResolver : IFormatterResolver
{
    public List<DurableAwaiter> _awaiters = new();
    
    public IMessagePackFormatter<T>? GetFormatter<T>() =>
        typeof(T) == typeof(DurableAwaiter)
            ? (IMessagePackFormatter<T>)new NewResultFormatter<DurableAwaiter>(_awaiters)
            : null;
}

class NewResultFormatter<T>(List<T> results) : IMessagePackFormatter<T> where T : new()
{
    public void Serialize(ref MessagePackWriter writer, T value, MessagePackSerializerOptions options)
    {
        writer.WriteNil();
    }

    public T Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        reader.Skip();
        var result = new T();
        results.Add(result);
        return result;
    }
}

class DurableAwaiter : INotifyCompletion
{
    [IgnoreMember] // No serialization exists and we wish to reconstruct the continuation flow anyway.
    private Action _continuation;

    public void GetResult() { }
    public bool IsCompleted { get; private set; }
    public void OnCompleted(Action continuation) => _continuation = continuation;
    public void SetResult()
    {
        IsCompleted = true;
        _continuation?.Invoke();
    }
}

[AsyncMethodBuilder(typeof(DurableAsyncMethodBuilder))]
class DurableTask
{
    [IgnoreMember] // we do not want to persist _canComplete = false. We should probably move this to the awaiter
    private bool _canComplete;
    
    public DurableTask()
    {
        _canComplete = true;
    }
    
    public DurableTask(bool canComplete)
    {
        _canComplete = canComplete;
    }
    
    private DurableAwaiter _awaiter = new();
    public DurableAwaiter GetAwaiter() => _awaiter;
    public void SetResult()
    {
        if(_canComplete)
        {
            _awaiter.SetResult();
        }
    }
}

// Represents a Durable stack where the first State is the innermost function.
// In a production scenario this should be a scoped variable somehow. Maybe use AsyncLocal?
class DurableStack
{
    public static List<byte[]> States = new();
}

class DurableAsyncMethodBuilder
{
    private DurableTask _task = new();
    
    public static DurableAsyncMethodBuilder Create() => new();
    public DurableTask Task => _task;

    public void SetStateMachine(IAsyncStateMachine stateMachine) { }
    public void SetResult() => _task.SetResult();
    public void SetException(Exception exception) { }

    public void Start<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
    {
        stateMachine.MoveNext();
    }
    
    public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        if (awaiter is DurableAwaiter)
        {
            DurableStack.States.Add(MessagePackSerializer.Typeless.Serialize(stateMachine));
        }
        else
        {
            awaiter.OnCompleted(stateMachine.MoveNext);
        }
    }

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        awaiter.UnsafeOnCompleted(stateMachine.MoveNext);
    }
}
