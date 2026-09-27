import angular from 'angular';
export const withText = (text: string, nodes: NodeList) => [...nodes].filter((n) => n.textContent!.includes(text))[0];
export const ctrl = <T>(name: string, el: HTMLElement) => angular.element(el).data()[`$${name}Controller`] as T;
