function SetMetadata(..._args: unknown[]) {
  return function (
    _target: unknown,
    _propertyKey?: string,
    _descriptor?: PropertyDescriptor,
  ) {};
}

export class DecoratedHandler {
  @SetMetadata('roles', ['admin', 'ops'])
  handle() {
    return true;
  }
}
